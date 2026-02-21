import { Injectable } from '@nestjs/common';

@Injectable()
export class ProjectionsService {
  getHello(): string {
    return 'Hello World!';
  }
}
