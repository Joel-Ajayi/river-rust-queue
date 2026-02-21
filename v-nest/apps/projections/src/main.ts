import { NestFactory } from '@nestjs/core';
import { ProjectionsModule } from './projections.module';

async function bootstrap() {
  const app = await NestFactory.create(ProjectionsModule);
  await app.listen(process.env.port ?? 3000);
}
bootstrap();
