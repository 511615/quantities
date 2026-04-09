export type FetchResolver = (
  url: string,
  init?: RequestInit,
) => Response | Promise<Response> | undefined;

export function jsonResponse(body: unknown, init?: ResponseInit) {
  return new Response(JSON.stringify(body), {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
    },
    ...init,
  });
}

export function createFetchMock(resolvers: FetchResolver[]) {
  return async (input: string | URL | Request, init?: RequestInit) => {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;

    for (const resolver of resolvers) {
      const response = await resolver(url, init);
      if (response) {
        return response;
      }
    }

    throw new Error(`Unhandled fetch URL: ${url}`);
  };
}
