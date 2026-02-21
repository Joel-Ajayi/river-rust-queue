import { Controller, Get } from '@nestjs/common';
import { WorkerEngineService } from './worker-engine.service';

@Controller()
export class WorkerEngineController {
  constructor(private readonly workerEngineService: WorkerEngineService) {}

  @Get()
  getHello(): string {
    return this.workerEngineService.getHello();
  }
}
