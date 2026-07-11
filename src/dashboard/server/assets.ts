import { existsSync, readdirSync, readFileSync } from "node:fs";
import { extname, join } from "node:path";

const CONTENT_TYPES: Record<string, string> = {
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".woff2": "font/woff2",
  ".woff": "font/woff",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".ico": "image/x-icon",
  ".png": "image/png",
};

export interface DashboardUi {
  indexHtml: string;
  assets: Map<string, { body: Buffer; type: string }>;
}

let uiCache: DashboardUi | undefined;

/**
 * Load the built UI. `baseDir` is the entry module's directory: the built
 * assets sit next to it (`dist/dashboard`), with the dev shell as a fallback,
 * so the same resolution works whether bundled or run from source.
 */
export const readDashboardUi = (baseDir: string): DashboardUi => {
  if (uiCache) return uiCache;
  const builtDirs = [join(baseDir, "dashboard"), join(baseDir, "..", "..", "dist", "dashboard")];
  for (const dir of builtDirs) {
    const index = join(dir, "index.html");
    if (!existsSync(index)) continue;
    const assets = new Map<string, { body: Buffer; type: string }>();
    const assetsDir = join(dir, "assets");
    if (existsSync(assetsDir)) {
      for (const name of readdirSync(assetsDir)) {
        assets.set(name, {
          body: readFileSync(join(assetsDir, name)),
          type: CONTENT_TYPES[extname(name)] ?? "application/octet-stream",
        });
      }
    }
    uiCache = { indexHtml: readFileSync(index, "utf8"), assets };
    return uiCache;
  }
  uiCache = {
    indexHtml: readFileSync(join(baseDir, "app", "index.html"), "utf8"),
    assets: new Map(),
  };
  return uiCache;
};

export const createAssetServer = (ui: DashboardUi, themeTag: string) => {
  const assetResponse = (name: string): Response | null => {
    const asset = ui.assets.get(name);
    if (!asset) return null;
    return new Response(asset.body, {
      status: 200,
      headers: {
        "content-type": asset.type,
        "cache-control": "public, max-age=31536000, immutable",
      },
    });
  };

  // The built UI uses relative URLs; a <base> tag pinned to the request path
  // makes both its hashed assets and the JSON API resolve under whatever path
  // the handler is mounted at.
  const htmlResponse = (pathname: string): Response => {
    const basePath = pathname.endsWith("/") ? pathname : `${pathname}/`;
    let body = ui.indexHtml.replace(/<base href="\.\/"\s*\/?>/, `<base href="${basePath}">`);
    if (themeTag) body = body.replace("</head>", `${themeTag}</head>`);
    return new Response(body, {
      status: 200,
      headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
    });
  };

  return { assetResponse, htmlResponse };
};
