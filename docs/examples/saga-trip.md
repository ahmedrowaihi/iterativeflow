# Saga: book a trip, compensate on failure

Book a hotel, then a car, then a flight. If anything fails, undo what's
already been booked. Compensation is just more steps in the flow — the
state machine lives in the graph.

```ts
type BookingIds = {
  tripId: string;
  hotelRef?: string;
  carRef?: string;
  flightRef?: string;
};

const bookTrip = flow("book-trip")
  .version(1)
  .input(
    z.object({
      tripId: z.string(),
      hotel: z.object({ checkIn: z.string(), city: z.string() }),
      car: z.object({ pickup: z.string(), location: z.string() }),
      flight: z.object({ depart: z.string(), origin: z.string(), dest: z.string() }),
    }),
  )

  .step(
    "book-hotel",
    ({ input }) =>
      hotelsAPI
        .book({
          idempotencyKey: `hotel:${input.tripId}`,
          ...input.hotel,
        })
        .then((ref) => ({ tripId: input.tripId, hotelRef: ref }) as BookingIds),
    { retries: 3, timeoutMs: 20_000, classify: classifyBookingError },
  )

  .step(
    "book-car",
    ({ input }) =>
      carsAPI
        .book({
          idempotencyKey: `car:${input.tripId}`,
          ...lookupCar(input.tripId),
        })
        .then((ref) => ({ ...input, carRef: ref }))
        .catch(async (err) => {
          if (classifyBookingError(err) === "permanent") {
            await hotelsAPI.cancel(input.hotelRef!).catch(noop);
            throw err; // rethrow so step fails terminal
          }
          throw err; // transient: let step retry
        }),
    { retries: 3, timeoutMs: 20_000 },
  )

  .step(
    "book-flight",
    ({ input }) =>
      flightsAPI
        .book({
          idempotencyKey: `flight:${input.tripId}`,
          ...lookupFlight(input.tripId),
        })
        .then((ref) => ({ ...input, flightRef: ref }))
        .catch(async (err) => {
          if (classifyBookingError(err) === "permanent") {
            await Promise.allSettled([
              hotelsAPI.cancel(input.hotelRef!),
              carsAPI.cancel(input.carRef!),
            ]);
            throw err;
          }
          throw err;
        }),
    { retries: 3, timeoutMs: 20_000 },
  )

  .step("confirm", ({ input }) =>
    notifications.send({
      idempotencyKey: `confirm:${input.tripId}`,
      tripId: input.tripId,
      refs: input,
    }),
  )

  .output(({ input }) => input)
  .build();

const classifyBookingError = (err: Error) =>
  err.message.includes("sold_out") || err.message.includes("invalid_card")
    ? ("permanent" as const)
    : ("transient" as const);

const noop = () => undefined;

engine.register(bookTrip);
```

Notes:

- Compensation lives inside the failing step's catch — fire `cancel`s on prior bookings, then rethrow so the step is `failed_terminal`.
- Compensation calls reuse the original `idempotencyKey`s so replay-then-fail doesn't double-cancel.
- Transient errors retry first; compensation only fires on permanent / retry-exhausted failures.
- Adding a 4th booking → bump `.version(2)`; v1 trips drain on the 3-booking graph.
