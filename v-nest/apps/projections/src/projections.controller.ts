import { Controller, Get } from '@nestjs/common';
import { ProjectionsService } from './projections.service';

@Controller()
export class ProjectionsController {
  constructor(private readonly projectionsService: ProjectionsService) {}

  @Get()
  getHello(): string {
    return this.projectionsService.getHello();
  }
}
