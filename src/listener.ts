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

  const options = stan
    .subscriptionOptions()
    .setManualAckMode(true)
    // Get a list of all events ever emitted. This will only be used the first time a service goes online if we also set durable name
    .setDeliverAllAvailable()
    // Ensure that our services only receive events that they haven't processed already
    .setDurableName("accounting-service"); // Durable name is usually the same as queue group name

  const subscription = stan.subscribe(
    "ticket:created",
    // Avoid sending the same message to duplicated services by placing them in the same queue group
    "order-service-queue-group", // Also ensures that durable name persists a disconnection to the NATS server
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
process.on("exit", () => stan.close());
