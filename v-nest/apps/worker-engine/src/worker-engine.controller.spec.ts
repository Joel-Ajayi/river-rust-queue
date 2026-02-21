import { Test, TestingModule } from '@nestjs/testing';
import { WorkerEngineController } from './worker-engine.controller';
import { WorkerEngineService } from './worker-engine.service';

describe('WorkerEngineController', () => {
  let workerEngineController: WorkerEngineController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [WorkerEngineController],
      providers: [WorkerEngineService],
    }).compile();

    workerEngineController = app.get<WorkerEngineController>(WorkerEngineController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(workerEngineController.getHello()).toBe('Hello World!');
    });
  });
});
