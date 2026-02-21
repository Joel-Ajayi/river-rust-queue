import { Test, TestingModule } from '@nestjs/testing';
import { ProjectionsController } from './projections.controller';
import { ProjectionsService } from './projections.service';

describe('ProjectionsController', () => {
  let projectionsController: ProjectionsController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [ProjectionsController],
      providers: [ProjectionsService],
    }).compile();

    projectionsController = app.get<ProjectionsController>(ProjectionsController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(projectionsController.getHello()).toBe('Hello World!');
    });
  });
});
