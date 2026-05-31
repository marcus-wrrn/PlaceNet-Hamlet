## What is PlaceNet?

PlaceNet is a networking infrastructure built on top of a Proof of Presence (PoP) protocol for creating secure Peer to Peer (P2P) connections with other services within a range of about 1-2km. This is achieved by broadcasting rotating encryption keys over Long Range Radio (LoRa) for both discovery and authentication. Upon verifying that another node is within broadcast distance (~600m - ~2km) using PoP, the nodes will establish a P2P connection via WireGuard with the help of a Coordination server accessible over the open internet. Once connected, PlaceNet nodes are able to share content between themselves as if they were on the same network.

Coordination servers operate on zero trust principles. They do not have the ability to see any content running through them and are planned on operating with a federated architecture inspired by the Fediverse/ActivityPub protocol, allowing users to run their own custom implementations. 

## What is a Hamlet?

A Hamlet serves as the 'brain' of a PlaceNet website. It is intended to be run on a Raspberry Pi, Mini PC or any other computer running on your local network. It is responsible for pairing with Beacons, handling the bulk of PoP, establishing/managing Wireguard connections and most importantly serving web content.

The Hamlet is built with the philosophy that it should be cross compatible with all forms of Web content. It should be able to serve not only static webpages but also allow for Video Streaming, Game Servers and all forms of Web Apps. A Hamlet node operates as a reverse proxy similar to nginx or caddy and is only responsible for managing PlaceNet connections before forwarding requests to self hosted content. It is responsible for handling TLS, Routing and PoP authentication.

## Current state of the project

The project is currently in early Alpha. PlaceNet nodes are able to send messages to each other over the internet but are unable to serve content or route through other services. NAT traversal is currently in the works. If you would like to support this project please consider making a contribution to the following projects:

[PlaceNet Beacon](https://github.com/marcus-wrrn/PlaceNet-Beacon)
- Device responsible for broadcasting PlaceNet messages over LoRa for authentication and discovery
- Currently only supports the LilyGO T-Beam but more device support is planned.
[PlaceNet Cloud Gateway](https://github.com/marcus-wrrn/PlaceNet-Cloud-Gateway)
- Coordination server responsible for establishing P2P connections and managing high level network changes

## Similar Projects

PlaceNet is greatly inspired by both Meshtastic and Tailscale. While PlaceNet differs in many ways, understanding what both of these projects are and how they work is really helpful for understanding the goal of PlaceNet.

[Meshatastic](https://meshtastic.org/docs/introduction/)
[Tailscale](https://tailscale.com/blog/how-tailscale-works)