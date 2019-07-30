import { Logger } from '@nestjs/common';
import { ClientProxy } from './client-proxy';
import { loadPackage } from '@nestjs/common/utils/load-package.util';
import { ReadPacket, SQSOptions, WritePacket } from '../interfaces';
import {
  SQS_DEFAULT_FIFO,
  SQS_DEFAULT_MAX_MESSAGES,
  SQS_DEFAULT_QUEUE,
  SQS_DEFAULT_REGION,
  SQS_DEFAULT_WAIT_TIME,
} from '../constants';
import { SQS as SQSType } from 'aws-sdk';

export class ClientSQS extends ClientProxy {
  protected readonly logger = new Logger(ClientProxy.name);
  private readonly region: string;
  private readonly queue: string;
  private readonly fifo: boolean;
  private readonly maxMessages: number;
  private readonly waitTime: number;
  private readonly sqs: SQSType;
  private queueUrl: string = '';

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
    const { SQS } = loadPackage('aws-sdk', ClientSQS.name, () => require('aws-sdk-js'));
    this.sqs = new SQS({ region: this.region });
  }

  close(): any {
    return true;
  }

  async connect(): Promise<any> {
    this.queueUrl = await this.getQueueUrl(this.queue);
    return true;
  }

  private async getQueueUrl(name: string) {
    const { QueueUrl } = await this.sqs.getQueueUrl({ QueueName: name }).promise();
    return QueueUrl;
  }

  protected dispatchEvent(packet: ReadPacket<any>): Promise<any> {
    return this.sqs.sendMessage({
      QueueUrl: this.queueUrl,
      MessageBody: JSON.stringify(packet),
    }).promise();
  }

  protected publish(packet: ReadPacket<any>, callback: (packet: WritePacket) => void): Function {
    return undefined;
  }

}
