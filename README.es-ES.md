

# iterativeflow v2

[![core](https://img.shields.io/npm/v/@iterativeflow/core?label=%40iterativeflow%2Fcore&labelColor=171717&color=FF570A)](https://www.npmjs.com/package/@iterativeflow/core)
[![license](https://img.shields.io/npm/l/@iterativeflow/core?labelColor=171717&color=FF570A)](../LICENSE)

Flujos de trabajo duraderos y agnósticos al backend para TypeScript.

Escribe un flujo como una función asíncrona ordinaria; sobrevive a caídas del proceso, reintenta pasos fallidos, permanece en suspensión durante días y reanuda de forma determinística al reproducir pasos memorizados. El mismo motor opera tras una interfaz `Backend` de cuatro puertos — **Postgres, SQLite, MySQL, MongoDB, Redis, DynamoDB, Cloudflare Durable Objects o en memoria** — residente o serverless. Publicado bajo el alcance `@iterativeflow/*` (versión más reciente `2.0.0-alpha.2`).

> Esta es la reescritura v2. La API v1 (`flow().step()` en graphile-worker) no ha cambiado y aún se entrega como [`iterativeflow`](../README.md).

```ts
import { createEngine, defineFlow, signalType } from "@iterativeflow/core";
import { createPgBackend, pgPool } from "@iterativeflow/postgres";
import { Pool } from "pg";

const onboard = defineFlow({
  name: "onboard",
  version: 1,
  signals: { survey: signalType<{ score: number }>() }, // declare the signal's payload type
  run: async (ctx, input: { userId: string }): Promise<{ score: number }> => {
    await ctx.step("create-account", () => createAccount(input.userId));
    await ctx.sleep(3 * 24 * 60 * 60_000); // 3 days, durable
    const survey = await ctx.signal("survey"); // typed { score: number }
    return { score: survey.score };
  },
});

const engine = createEngine(createPgBackend(pgPool(new Pool())), [onboard]);
const stop = engine.run(); // resident worker loop; returns a stop fn

const handle = await engine.submit(onboard, { userId: "u_1" });
// 3 days later, from a webhook:
await engine.signal(handle, "survey", { score: 9 });
const { output } = await engine.result(handle); // { score: 9 }
await stop();
```

Esa ejecución reside en tu backend durante tres días. Los workers pueden fallar, los despliegues pueden revertirse, el proceso puede ser terminado y reiniciado: cuando el temporizador se dispare, el flujo reanudará desde donde se quedó, reproduciendo el paso memorizado `create-account` en lugar de volver a ejecutarlo.

- **Pasos** memorizados por `(runId, cursor)` — `ctx.step(label, fn)`, la unidad de ejecución al menos una vez
- **Suspensos** y **señales** externas que duran días — `ctx.sleep(ms)` / `ctx.signal(name)`
- **`ctx.invoke(child, input)`** para flujos secundarios, y `ctx.invoke([…])` para abanico paralelo y unión
- **Contratos tipados** — `submit` devuelve un `RunHandle<Output, Signals>`; las salidas y cargas útiles de las señales están tipadas de extremo a extremo
- **Al menos una vez** mediante una outbox transaccional confirmada con cada paso; un reconciliador reactiva cualquier elemento varado por una caída
- **Serverless o residente** — `engine.run()` para un bucle de worker, o `serverlessTick` para un ciclo acotado por invocación de Lambda/Vercel/Cron
- **Concurrencia estructurada** — una ejecución que termina sin éxito cancela a sus descendientes

## Paquetes

| Paquete                                                      | Qué es                                                                                                        |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| [`@iterativeflow/core`](packages/core)                       | El motor — `defineFlow`, `ctx`, `createEngine`, reproducción, outbox. Combínalo con un backend.                |
| [`@iterativeflow/memory`](packages/memory)                   | Backend en memoria — pruebas, desarrollo, un solo proceso.                                                    |
| [`@iterativeflow/postgres`](packages/postgres)               | Backend de Postgres — outbox transaccional, push opcional `LISTEN/NOTIFY`, amigable con serverless. **El predeterminado.** |
| [`@iterativeflow/sqlite`](packages/sqlite)                   | Backend de SQLite (`@libsql/client`) — embebido, Turso o servicio de un solo nodo.                            |
| [`@iterativeflow/mysql`](packages/mysql)                     | Backend de MySQL/InnoDB — reclamaciones `FOR UPDATE SKIP LOCKED`.                                              |
| [`@iterativeflow/mongodb`](packages/mongodb)                 | Backend de MongoDB — outbox transaccional multidocumento (se requiere conjunto de réplicas).                  |
| [`@iterativeflow/redis`](packages/redis)                     | Backend de Redis — outbox con scripts Lua, nodo único.                                                        |
| [`@iterativeflow/dynamodb`](packages/dynamodb)               | Backend de DynamoDB — tabla única, outbox `TransactWriteItems`, serverless en AWS.                             |
| [`@iterativeflow/durable-objects`](packages/durable-objects) | Ejecuta el motor **dentro de un Cloudflare Durable Object** en su SQLite incorporado — sin base de datos externa. |
| [`@iterativeflow/webhooks`](packages/webhooks)               | Borde de entrada — verifica un webhook firmado del proveedor y lo entrega como una señal duradera que un flujo aparcado espera. |
| [`@iterativeflow/dashboard`](packages/dashboard)             | UI de operaciones sin dependencias (lista de ejecuciones, detalles, cancelar/reintentar/señal) como un controlador fetch. |
| [`@iterativeflow/conformance`](packages/conformance)         | Los suites compartidos que cada backend debe aprobar — la definición ejecutable de un backend correcto.        |

Cada backend implementa los mismos cuatro puertos y pasa los mismos nueve suites de conformidad, por lo que intercambiar uno por otro solo cambia la llamada `create*Backend(...)`.

### ¿Qué backend?

- **`memory`** — pruebas, ejemplos, una aplicación de un solo proceso.
- **`postgres`** — el predeterminado: consistencia fuerte, finalización por push mediante `LISTEN/NOTIFY`, funciona en Postgres serverless/por pool.
- **`sqlite`** — embebido o edge, un archivo o Turso/libsql; sin servidor.
- **`durable-objects`** — edge, fuertemente consistente por objeto, sin base de datos externa.
- **`redis`** — baja latencia, nodo único (también funcionan Valkey/Dragonfly).
- **`mysql` / `mongodb` / `dynamodb`** — cuando ese almacén ya forma parte de tu stack.

## Instalación y inicio rápido

```bash
npm install @iterativeflow/core @iterativeflow/memory
```

```ts
import { createEngine, defineFlow } from "@iterativeflow/core";
import { createMemoryBackend } from "@iterativeflow/memory";

const double = defineFlow<{ x: number }, number>({
  name: "double",
  version: 1,
  run: async (ctx, input) => ctx.step("d", () => input.x * 2),
});

const engine = createEngine(createMemoryBackend(), [double]);
const handle = await engine.submit(double, { x: 21 });
const stop = engine.run();
const res = await engine.result(handle, { timeoutMs: 5_000 });
await stop();
// res.output === 42
```

Cambia `createMemoryBackend()` por `createPgBackend(...)`, `createSqliteBackend(...)`, etc. — nada más cambia. El README de cada backend cubre su configuración de conexión y `applySchema`.

## El contexto del flujo

Dentro de `run`, `ctx` es la superficie duradera; cada llamada es un punto de control:

- `ctx.step(label, fn)` — ejecuta `fn` una vez; memoriza su resultado y reprodúcelo en cada intento posterior.
- `ctx.sleep(ms)` — suspende y reanuda después de un temporizador duradero.
- `ctx.signal(name)` — aparca hasta que una `engine.signal(runId, name, payload)` externa (o [`@iterativeflow/webhooks`](packages/webhooks)) entregue una carga útil tipada.
- `ctx.invoke(flow, input)` — ejecuta un flujo secundario y espera su salida; pasa un array para expandir en paralelo y unir las salidas en orden.

La reproducción asume que el cuerpo es estable: cada registro de memoria de paso guarda una huella de forma, y una redistribución que reordena el cuerpo activa la `driftPolicy` del flujo (aparcar o fallar) en lugar de ejecutar el paso incorrecto. Mantén el orden y las etiquetas de los pasos estables entre despliegues; incrementa `version` cuando el cuerpo cambie significativamente.

### Constructor (encadenado, tipado)

¿Prefieres una cadena? `builder` se compila al mismo `ctx.step` — cada resultado `.step` se añade a un acumulador tipado (`acc.account`, `acc.survey`) que pasos posteriores y la proyección de salida pueden leer. Las esperas, señales e invocaciones ocurren a través de `ctx` dentro de un paso (no hay nodos de cadena separados para ellos):

```ts
import { builder } from "@iterativeflow/core";

const onboard = builder<{ userId: string }>("onboard", 1)
  .step("account", (acc) => createAccount(acc.input.userId))
  .step("survey", async (_acc, ctx) => {
    await ctx.sleep(3 * 24 * 60 * 60_000); // 3 days
    return (await ctx.signal("survey")) as { score: number };
  })
  .output((acc) => ({ score: acc.survey.score }));
```

## Documentación

- [ARQUITECTURA](../docs/v2/ARCHITECTURE.md) — los cuatro puertos + outbox transaccional
- [CONTRATOS](../docs/v2/CONTRACTS.md) — flujos y señales tipados
- [MIGRACIÓN](../docs/v2/MIGRATION.md) — propiedad del esquema, residente vs. serverless
- [PARIDAD](../docs/v2/PARITY.md) — paridad de características v1 → v2

## Licencia

MIT
