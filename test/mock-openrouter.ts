import { createServer, type IncomingHttpHeaders, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";

export type Reply =
  | { kind: "stream"; events: (object | string)[]; endWithoutDone?: boolean }
  | { kind: "json"; status: number; body: unknown; headers?: Record<string, string> }
  | { kind: "hang" };

export interface Received {
  headers: IncomingHttpHeaders;
  body: { model: string } & Record<string, unknown>;
}

export interface MockOpenRouter {
  url: string;
  received: Received[];
  models: unknown[];
  reply(model: string, ...replies: Reply[]): void;
  close(): Promise<void>;
}

const chunk = (model: string, delta: object, finish: string | null, extra: object = {}) => ({
  id: "gen-mock-1",
  object: "chat.completion.chunk",
  created: 1,
  model,
  choices: [{ index: 0, delta, finish_reason: finish, native_finish_reason: finish }],
  ...extra,
});

export const usage = (prompt: number, completion: number, cost?: number) => ({
  prompt_tokens: prompt,
  completion_tokens: completion,
  total_tokens: prompt + completion,
  prompt_tokens_details: { cached_tokens: 0 },
  completion_tokens_details: { reasoning_tokens: 0 },
  ...(cost === undefined ? {} : { cost }),
});

export function streamOf(
  model: string,
  pieces: string[],
  options: { usage?: object; finish?: string } = {},
): Reply {
  const finish = options.finish ?? "stop";
  return {
    kind: "stream",
    events: [
      ": OPENROUTER PROCESSING",
      ...pieces.map((content) => chunk(model, { role: "assistant", content }, null)),
      chunk(model, { role: "assistant", content: "" }, finish),
      chunk(model, { role: "assistant", content: "" }, finish, {
        usage: options.usage ?? usage(10, pieces.length, 0.000042),
      }),
      "[DONE]",
    ],
  };
}

export function streamError(
  model: string,
  pieces: string[],
  error: { code: number | string; message: string; metadata?: object },
): Reply {
  return {
    kind: "stream",
    events: [
      ...pieces.map((content) => chunk(model, { content }, null)),
      chunk(model, { content: "" }, "error", { error }),
    ],
  };
}

export const errorReply = (status: number, errorType?: string, metadata: object = {}): Reply => ({
  kind: "json",
  status,
  body: {
    error: {
      code: status,
      message: "Mock error",
      metadata: { ...(errorType === undefined ? {} : { error_type: errorType }), ...metadata },
    },
  },
});

export const defaultModels: unknown[] = [
  {
    id: "mock/small",
    context_length: 8192,
    pricing: { prompt: "0.000001", completion: "0.000002" },
  },
  {
    id: "mock/large",
    context_length: 128000,
    pricing: {
      prompt: "0.0000025",
      completion: "0.00001",
      request: "0",
      overrides: [{ min_prompt_tokens: 200000, prompt: "0.000005", completion: "0.00002" }],
    },
  },
  { id: "mock/free:free", context_length: 4096, pricing: { prompt: "0", completion: "0" } },
  { id: "openrouter/auto", pricing: { prompt: "-1", completion: "-1" } },
  { id: "bad id", pricing: { prompt: "0", completion: "0" } },
];

async function send(response: ServerResponse, reply: Reply): Promise<void> {
  if (reply.kind === "hang") return;
  if (reply.kind === "json") {
    response.writeHead(reply.status, { "content-type": "application/json", ...reply.headers });
    response.end(JSON.stringify(reply.body));
    return;
  }
  response.writeHead(200, { "content-type": "text/event-stream", "x-generation-id": "gen-mock-1" });
  const text = reply.events
    .map((event) =>
      typeof event === "string"
        ? event.startsWith(":")
          ? `${event}\n\n`
          : `data: ${event}\n\n`
        : `data: ${JSON.stringify(event)}\n\n`,
    )
    .join("");
  for (let start = 0; start < text.length; start += 7) {
    response.write(text.slice(start, start + 7));
    await new Promise((resolve) => setImmediate(resolve));
  }
  response.end();
}

export async function startMockOpenRouter(port = 0): Promise<MockOpenRouter> {
  const received: Received[] = [];
  const scripts = new Map<string, Reply[]>();
  const models = [...defaultModels];
  const server = createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", (data: Buffer) => chunks.push(data));
    request.on("end", () => {
      if (request.method === "GET" && request.url === "/api/v1/models") {
        void send(response, { kind: "json", status: 200, body: { data: models } });
        return;
      }
      if (request.method !== "POST" || request.url !== "/api/v1/chat/completions") {
        void send(response, errorReply(404, "not_found"));
        return;
      }
      const body = JSON.parse(Buffer.concat(chunks).toString()) as Received["body"];
      received.push({ headers: request.headers, body });
      const reply = scripts.get(body.model)?.shift() ?? streamOf(body.model, ["Mock ", "answer."]);
      void send(response, reply);
    });
  });
  await new Promise<void>((resolve) => server.listen(port, "127.0.0.1", resolve));
  const { port: bound } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${String(bound)}/api/v1`,
    received,
    models,
    reply(model, ...replies) {
      scripts.set(model, [...(scripts.get(model) ?? []), ...replies]);
    },
    close: () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(() => {
          resolve();
        });
      }),
  };
}

if (import.meta.main) {
  const mock = await startMockOpenRouter(Number(process.env.PORT ?? 8090));
  process.stdout.write(`mock OpenRouter at ${mock.url}\n`);
}
