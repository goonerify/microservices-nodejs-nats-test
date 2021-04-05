import nats from "node-nats-streaming";
import { randomBytes } from "crypto";
import { TicketCreatedListener } from "./events/ticket-created-listener";

console.clear(); // Clear previous console messages before printing new ones

const stan = nats.connect("ticketing", randomBytes(4).toString("hex"), {
  url: "http://localhost:4222",
});

stan.on("connect", () => {
  console.log("Listener connected to NATS");

  // Prevent sending of events once this NATS streaming server client goes offline
  // This will theoretically help NATS streaming server to understand that it
  // should spin up a new client right away and not send heartbeat checks to see
  // if this old one will restart, which should prevent message delivery delays
  stan.on("close", () => {
    console.log("NATS connection closed");
    process.exit();
  });

  new TicketCreatedListener(stan).listen();
});

// Watch for interrupt / terminate signals and close our channel
// These events 'SIGINT', 'SIGTERM' might be a little finicky on windows
process.on("SIGINT", () => stan.close());
process.on("SIGTERM", () => stan.close());
process.on("exit", () => stan.close());
