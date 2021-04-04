import nats, { Message } from "node-nats-streaming";
import { randomBytes } from "crypto";

console.clear();

const stan = nats.connect("ticketing", randomBytes(4).toString("hex"), {
  url: "http://localhost:4222",
});

stan.on("connect", () => {
  console.log("Listener connected to NATS");

  // Prevent sending of events once the server goes offline
  stan.on("close", () => {
    console.log("NATS connection closed");
    process.exit();
  });

  const options = stan.subscriptionOptions().setManualAckMode(true);
  const subscription = stan.subscribe(
    "ticket:created",
    // Avoid sending the same message to duplicated services by placing them in the same queue group
    "order-service-queue-group",
    options
  );

  subscription.on("message", (msg: Message) => {
    const data = msg.getData();

    if (typeof data === "string") {
      console.log(`Received event #${msg.getSequence()}, with data: ${data}`);
    }

    msg.ack();
  });
});

// Watch for interrupt / terminate signals and close our channel
// These events 'SIGINT', 'SIGTERM' might be a little finicky on windows
process.on("SIGINT", () => stan.close());
process.on("SIGTERM", () => stan.close());
