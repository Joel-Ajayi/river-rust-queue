import { Module } from '@nestjs/common';
import { ProjectionsController } from './projections.controller';
import { ProjectionsService } from './projections.service';

@Module({
  imports: [],
  controllers: [ProjectionsController],
  providers: [ProjectionsService],
})
export class ProjectionsModule {}
