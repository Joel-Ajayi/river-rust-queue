import { Injectable } from '@nestjs/common';

@Injectable()
export class WorkerEngineService {
  getHello(): string {
    return 'Hello World!';
  }
}
