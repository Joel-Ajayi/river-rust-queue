import { Module } from '@nestjs/common';
import { WorkerEngineController } from './worker-engine.controller';
import { WorkerEngineService } from './worker-engine.service';

@Module({
  imports: [],
  controllers: [WorkerEngineController],
  providers: [WorkerEngineService],
})
export class WorkerEngineModule {}
