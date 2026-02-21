import { NestFactory } from '@nestjs/core';
import { WorkerEngineModule } from './worker-engine.module';

async function bootstrap() {
  const app = await NestFactory.create(WorkerEngineModule);
  await app.listen(process.env.port ?? 3000);
}
bootstrap();
