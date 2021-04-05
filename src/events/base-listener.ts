import { Message, Stan } from "node-nats-streaming";
import { Subjects } from "./subjects";

interface Event {
  subject: Subjects;
  data: any;
}

export abstract class Listener<T extends Event> {
  abstract subject: T["subject"];
  abstract queueGroupName: string;
  abstract onMessage(data: T["data"], msg: Message): void;
  private client: Stan;
  protected ackWait = 5 * 1000;

  constructor(client: Stan) {
    this.client = client;
  }

  subscriptionOptions() {
    return (
      this.client
        .subscriptionOptions()
        // Get a list of all events ever emitted. This will only be used the first time a service goes online if we also set durable name
        .setDeliverAllAvailable()
        .setManualAckMode(true)
        .setAckWait(this.ackWait)
        // Ensure that our services only receive events that they haven't processed already
        .setDurableName(this.queueGroupName)
    ); // Durable name is usually the same as queue group name
  }

  listen() {
    const subscription = this.client.subscribe(
      this.subject,
      // Avoid sending the same message to duplicated services by placing them in the same queue group
      this.queueGroupName, // Also ensures that durable name persists a disconnection to the NATS server
      this.subscriptionOptions()
    );

    subscription.on("message", (msg: Message) => {
      console.log(`Message received: ${this.subject} / ${this.queueGroupName}`);

      const parsedData = this.parseMessage(msg);
      this.onMessage(parsedData, msg);
    });
  }

  parseMessage(msg: Message) {
    const data = msg.getData();
    return typeof data === "string"
      ? JSON.parse(data)
      : JSON.parse(data.toString("utf8")); // parsing a buffer. Not expected to be used
  }
}
