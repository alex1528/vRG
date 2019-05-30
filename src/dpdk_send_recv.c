#include <stdint.h>
#include <inttypes.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_cycles.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_ether.h>
#include <rte_arp.h>
#include <rte_ip.h>
#include <rte_udp.h>
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <rte_memcpy.h>
#include "pppoeclient.h"

#define RX_RING_SIZE 128

#define TX_RING_SIZE 512

#define BURST_SIZE 32

extern tPPP_PORT				ppp_ports[USER];
extern struct rte_mempool 		*mbuf_pool;
extern struct rte_ring 			*rte_ring;

static uint16_t nb_rxd = RX_RING_SIZE;
static uint16_t nb_txd = TX_RING_SIZE;

static const struct rte_eth_conf port_conf_default = {
	.rxmode = { .max_rx_pkt_len = ETHER_MAX_LEN }
};

typedef struct user_addr {
	unsigned char 	mac_addr[6];
	uint32_t 		ip_addr;
	BOOL			is_fill;
}user_addr_t;

user_addr_t user_addr_table[10];

int PPP_PORT_INIT(uint16_t port)
{
	struct rte_eth_conf port_conf = port_conf_default;
	struct rte_eth_dev_info dev_info;
	const uint16_t rx_rings = 1, tx_rings = 2;
	int retval;
	uint16_t q;

	if (!rte_eth_dev_is_valid_port(port))
		return -1;
	rte_eth_dev_info_get(port, &dev_info);
	if (dev_info.tx_offload_capa & DEV_TX_OFFLOAD_MBUF_FAST_FREE)
		port_conf.txmode.offloads |= DEV_TX_OFFLOAD_MBUF_FAST_FREE;

	retval = rte_eth_dev_configure(port, rx_rings, tx_rings, &port_conf);
	if (retval != 0)
		return retval;
	retval = rte_eth_dev_adjust_nb_rx_tx_desc(port, &nb_rxd,&nb_txd);
	if (retval < 0)
		rte_exit(EXIT_FAILURE,"Cannot adjust number of descriptors: err=%d, ""port=%d\n", retval, port);

	/* Allocate and set up 1 RX queue per Ethernet port. */
	for(q=0; q<rx_rings; q++) {
		retval = rte_eth_rx_queue_setup(port,q,nb_rxd,rte_eth_dev_socket_id(port),NULL,mbuf_pool);
		if (retval < 0)
			return retval;
	}

	/* Allocate and set up 2 TX queue per Ethernet port. */
	for(q=0; q<tx_rings; q++) {
		retval = rte_eth_tx_queue_setup(port,q,nb_txd,rte_eth_dev_socket_id(port), NULL);
		if (retval < 0)
			return retval;
	}

	/* Start the Ethernet port. */
	retval = rte_eth_dev_start(port);
	if (retval < 0)
		return retval;
	//rte_eth_promiscuous_enable(port);
	return 0;
}

int ppp_recvd(void)
{
	struct rte_mbuf 	*single_pkt;
	uint64_t 			total_tx;
	struct ether_hdr 	*eth_hdr;
	struct rte_mbuf 	*pkt[BURST_SIZE];
	tPPP_MBX 			*mail = malloc(sizeof(tPPP_MBX));
	
	for(;;) {
		uint16_t nb_rx = rte_eth_rx_burst(1,0,pkt,BURST_SIZE);
		if (nb_rx == 0)
			continue;
		total_tx = 0;
		for(int i=0; i<nb_rx; i++) {
			single_pkt = pkt[i];
			rte_prefetch0(rte_pktmbuf_mtod(single_pkt, void *));
			eth_hdr = rte_pktmbuf_mtod(single_pkt,struct ether_hdr*);
			if (eth_hdr->ether_type != htons(0x8864) && eth_hdr->ether_type != htons(0x8863)) {
				rte_pktmbuf_free(single_pkt);
				continue;
			}
			ppp_payload_t *ppp_payload = ((ppp_payload_t *)((char *)eth_hdr + sizeof(struct ether_hdr) + sizeof(pppoe_header_t)));
			if (unlikely(eth_hdr->ether_type == htons(0x8863) || (eth_hdr->ether_type == htons(0x8864) && (ppp_payload->ppp_protocol == htons(LCP_PROTOCOL) || ppp_payload->ppp_protocol == htons(AUTH_PROTOCOL) || ppp_payload->ppp_protocol == htons(IPCP_PROTOCOL))))) {
				rte_memcpy(mail->refp,eth_hdr,single_pkt->data_len);
				mail->type = IPC_EV_TYPE_DRV;
				mail->len = single_pkt->data_len;
				//enqueue eth_hdr single_pkt->data_len
				uint8_t ret = rte_ring_enqueue_burst(rte_ring,&mail,1,NULL);
				rte_pktmbuf_free(single_pkt);
				continue;
			}
			rte_memcpy(eth_hdr->s_addr.addr_bytes,ppp_ports[0].lan_mac,6);
			rte_memcpy(eth_hdr->d_addr.addr_bytes,user_addr_table[0].mac_addr,6);
			eth_hdr->ether_type = ppp_payload->ppp_protocol;
			rte_memcpy((char *)eth_hdr+8,eth_hdr,sizeof(struct ether_hdr));
			single_pkt->data_off += 8;
			single_pkt->pkt_len -= 8;
			single_pkt->data_len -= 8;
			pkt[total_tx++] = single_pkt;
		}
		if (likely(total_tx > 0)) {
			uint16_t nb_tx = rte_eth_tx_burst(0,0,pkt,total_tx);
			if (unlikely(nb_tx < total_tx)) {
				for(uint16_t buf=nb_tx; buf<total_tx; buf++)
					rte_pktmbuf_free(pkt[buf]);
			}
		}
	}
	return 0;

}

int control_plane_dequeue(tPPP_MBX **mail)
{
	uint16_t burst_size;

	for(;;) {
		burst_size = rte_ring_dequeue_burst(rte_ring,mail,BURST_SIZE,NULL);
		if (likely(burst_size == 0)) {
			continue;
		}
		printf("recv %u ring msg\n", burst_size);
		break;
	}
	return burst_size;
}

int encapsulation(void)
{
	struct rte_mbuf 	*single_pkt;
	uint64_t 			total_tx;
	struct ether_hdr 	*eth_hdr;
	pppoe_header_t 		*pppoe_header;
	struct rte_mbuf 	*pkt[BURST_SIZE];
	unsigned char 		mac_addr[6];
	struct arp_hdr		*arphdr;

	rte_eth_macaddr_get(0,(struct ether_addr *)mac_addr);
	while(data_plane_start == FALSE)
		usleep(1000);
	for(;;) {
		uint16_t nb_rx = rte_eth_rx_burst(0,0,pkt,BURST_SIZE);
		if (nb_rx == 0)
			continue;
		total_tx = 0;
		for(int i=0; i<nb_rx; i++) {
			single_pkt = pkt[i];
			rte_prefetch0(rte_pktmbuf_mtod(single_pkt,void *));
			eth_hdr = rte_pktmbuf_mtod(single_pkt,struct ether_hdr*);
			if (unlikely(eth_hdr->ether_type == htons(0x0806))) {
				rte_memcpy(eth_hdr->d_addr.addr_bytes,eth_hdr->s_addr.addr_bytes,6);
				rte_memcpy(eth_hdr->s_addr.addr_bytes,mac_addr,6);
				arphdr = (struct arp_hdr *)(rte_pktmbuf_mtod(single_pkt, unsigned char *) + sizeof(struct ether_hdr));
				if (arphdr->arp_op == htons(0x0001) && arphdr->arp_data.arp_tip == ppp_ports[0].ipv4_gw) {
					/* record ip mac match table */
					rte_memcpy(user_addr_table[0].mac_addr,eth_hdr->d_addr.addr_bytes,ETH_ALEN);
					rte_memcpy(user_addr_table[0].ip_addr,arphdr->arp_data.arp_tip,ETH_ALEN);
					user_addr_table[0].is_fill = TRUE;

					rte_memcpy(arphdr->arp_data.arp_tha.addr_bytes,arphdr->arp_data.arp_sha.addr_bytes,6);
					rte_memcpy(arphdr->arp_data.arp_sha.addr_bytes,mac_addr,6);
					arphdr->arp_data.arp_tip = arphdr->arp_data.arp_sip;
					arphdr->arp_data.arp_sip = ppp_ports[0].ipv4_gw;
					arphdr->arp_op = htons(0x0002);
					rte_eth_tx_burst(0,0,&single_pkt,1);
					continue;
				}
				rte_pktmbuf_free(single_pkt);
				continue;
			}
			if (unlikely(eth_hdr->ether_type == htons(0x8863) || (eth_hdr->ether_type == htons(0x8864)))) {
				rte_pktmbuf_free(single_pkt);
				continue;
			}
			rte_memcpy(eth_hdr->s_addr.addr_bytes,ppp_ports[0].src_mac,6);
			rte_memcpy(eth_hdr->d_addr.addr_bytes,ppp_ports[0].dst_mac,6);

			uint16_t protocol = eth_hdr->ether_type;
			eth_hdr->ether_type = htons(0x8864);
			char *cur = (char *)eth_hdr - 8;
			rte_memcpy(cur,eth_hdr,14);
			pppoe_header = (pppoe_header_t *)(cur+14);
			pppoe_header->ver_type = 0x11;
			pppoe_header->code = 0;
			pppoe_header->session_id = ppp_ports[0].session_id;
			pppoe_header->length = htons((single_pkt->pkt_len) - 14 + 2);
			*((uint16_t *)(cur+14+sizeof(pppoe_header_t))) = protocol;
			single_pkt->data_off -= 8;
			single_pkt->pkt_len += 8;
			single_pkt->data_len += 8;
			pkt[total_tx++] = single_pkt;
		}
		if (likely(total_tx > 0)) {
			uint16_t nb_tx = rte_eth_tx_burst(1,0,pkt,total_tx);
			if (unlikely(nb_tx < total_tx)) {
				for(uint16_t buf=nb_tx; buf<total_tx; buf++)
					rte_pktmbuf_free(pkt[buf]);
			}
		}
	}
	return 0;

}

void drv_xmit(U8 *mu, U16 mulen)
{
	struct rte_mbuf *pkt;
	char 			*buf;

	pkt = rte_pktmbuf_alloc(mbuf_pool);
	buf = rte_pktmbuf_mtod(pkt,char *);
	rte_memcpy(buf,mu,mulen);
	pkt->data_len = mulen;
	pkt->pkt_len = mulen;
	
	uint16_t nb_tx = rte_eth_tx_burst(1,1,&pkt,1);
	rte_pktmbuf_free(pkt);
}
