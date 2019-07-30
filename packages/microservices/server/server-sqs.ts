import { Server } from './server';
import { CustomTransportStrategy, SQSOptions } from '../interfaces';
import {
  NO_EVENT_HANDLER,
  SQS_DEFAULT_FIFO,
  SQS_DEFAULT_MAX_MESSAGES,
  SQS_DEFAULT_QUEUE,
  SQS_DEFAULT_REGION,
  SQS_DEFAULT_WAIT_TIME,
} from '../constants';
import { SQS as SQSType } from 'aws-sdk';
import { Message } from 'aws-sdk/clients/sqs';
import { isString } from '@nestjs/common/utils/shared.utils';
import { Observable } from 'rxjs';

export class ServerSQS extends Server implements CustomTransportStrategy {
  private readonly region: string;
  private readonly queue: string;
  private readonly fifo: boolean;
  private readonly maxMessages: number;
  private readonly waitTime: number;
  private readonly sqs: SQSType;
  private startedPolling: boolean;
  private queueUrl: string = null;
  private receiveParams: any = {};

  constructor(private readonly options: SQSOptions['options']) {
    super();
    this.region = this.getOptionsProp(this.options, 'region') || SQS_DEFAULT_REGION;
    this.queue = this.getOptionsProp(this.options, 'queue') || SQS_DEFAULT_QUEUE;
    this.fifo = this.getOptionsProp(this.options, 'fifo') || SQS_DEFAULT_FIFO;
    this.maxMessages = this.getOptionsProp(this.options, 'maxMessages') || SQS_DEFAULT_MAX_MESSAGES;
    if (this.maxMessages > SQS_DEFAULT_MAX_MESSAGES) {
      this.maxMessages = SQS_DEFAULT_MAX_MESSAGES;
    }
    this.waitTime = this.getOptionsProp(this.options, 'waitTime') || SQS_DEFAULT_WAIT_TIME;
    const { SQS } = this.loadPackage('aws-sdk', ServerSQS.name, () => require('aws-sdk-js'));
    this.sqs = new SQS({ region: this.region });
    this.startedPolling = true;
  }

  close(): any {
    this.startedPolling = false;
  }

  listen(callback: () => void): any {
  }

  protected async start() {
    this.queueUrl = await this.getQueueUrl(this.queue);
    this.receiveParams = {
      QueueUrl: this.queueUrl,
      AttributeNames: [
        'ApproximateReceiveCount',
      ],
      MessageAttributeNames: [
        'All',
      ],
      MaxNumberOfMessages: this.maxMessages,
      WaitTimeSeconds: this.waitTime,
    };
  }

  private async pollIndefinitely() {
    while (this.startedPolling) {
      const { Messages = [] } = await this.sqs.receiveMessage(this.receiveParams).promise();
      if (Messages.length !== 0) {
        Messages.forEach(message => {
          this.handleMessage(message);
        });
      }
    }
  }

  private async handleMessage(message: Message) {
    const { Body, ReceiptHandle } = message;
    const packet = JSON.parse(Body.toString());
    const pattern = isString(packet.pattern)
      ? packet.pattern
      : JSON.stringify(packet.pattern);
    const handler = this.getHandlerByPattern(pattern);
    if (!handler) {
      return this.logger.error(NO_EVENT_HANDLER);
    }

    const response$ = this.transformToObservable(
      await handler(packet.data),
    ) as Observable<any>;

    const publish = <T>() =>
      this.deleteMessage(ReceiptHandle);

    response$ && this.send(response$, publish);
  }

  private async getQueueUrl(name: string) {
    const { QueueUrl } = await this.sqs.getQueueUrl({ QueueName: name }).promise();
    return QueueUrl;
  }

  private async deleteMessage(handle: string) {
    await this.sqs.deleteMessage({
      QueueUrl: this.queueUrl,
      ReceiptHandle: handle,
    }).promise();
  }
}
