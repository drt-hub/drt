import { expect, type Page, test } from "@playwright/test";

const deterministicFonts = `
  @font-face {
    font-family: "drt-visual-sans";
    src: url("/assets/visual-sans.woff2") format("woff2");
    font-style: normal;
    font-weight: 100 900;
    font-display: block;
  }
  @font-face {
    font-family: "drt-visual-mono";
    src: url("/assets/visual-mono.woff2") format("woff2");
    font-style: normal;
    font-weight: 100 900;
    font-display: block;
  }
  :root {
    --sans: "drt-visual-sans", sans-serif !important;
    --mono: "drt-visual-mono", monospace !important;
  }
`;

async function openDocsPage(page: Page, path: string): Promise<void> {
  const response = await page.goto(path, { waitUntil: "networkidle" });
  expect(response?.ok()).toBeTruthy();
  await expect(page.locator("main")).toBeVisible();
  await page.addStyleTag({ content: deterministicFonts });
  await page.evaluate(async () => {
    await document.fonts.ready;
  });
}

async function expectStableScreenshot(
  page: Page,
  name: string,
  fullPage = true,
): Promise<void> {
  await expect(page).toHaveScreenshot(name, {
    animations: "disabled",
    caret: "hide",
    fullPage,
    scale: "css",
    // Chromium rasterizes a small number of glyph-edge pixels differently
    // across Linux hosts even with identical bundled fonts. Keep the budget
    // below one percent so layout, color, spacing, and content regressions
    // still fail while host antialiasing noise does not.
    maxDiffPixelRatio: 0.005,
  });
}

test("overview page @desktop", async ({ page }) => {
  await openDocsPage(page, "/index.html");
  await expect(page.locator("header [data-scale-filter-root]")).toHaveCount(0);
  await expect(page.locator("main [data-scale-filter-root]")).toBeVisible();
  await expect(page.locator(".overview-sync-row")).toHaveCount(10);
  await expectStableScreenshot(page, "overview.png");
});

test("lineage DAG @desktop", async ({ page }) => {
  await openDocsPage(page, "/dag.html");
  await expectStableScreenshot(page, "dag.png");
});

test("sync definition and lineage tabs @desktop", async ({ page }) => {
  await openDocsPage(page, "/sync/orders-to-pg.html");
  await expectStableScreenshot(page, "sync-definition.png");

  await page.getByRole("tab", { name: "Lineage" }).click();
  await expect(page.getByRole("tab", { name: "Lineage" })).toHaveAttribute(
    "aria-selected",
    "true",
  );
  await expectStableScreenshot(page, "sync-lineage.png");
});

test("overview page @mobile", async ({ page }) => {
  await openDocsPage(page, "/index.html");
  const table = page.locator(".overview-sync-table");
  await expect(table.locator("a")).toHaveCount(10);
  await table.scrollIntoViewIfNeeded();
  await expect(table.locator(".overview-sync-row").first()).toHaveCSS(
    "display",
    "block",
  );
  await expectStableScreenshot(page, "overview.png", false);
});

test("sync definition @mobile", async ({ page }) => {
  await openDocsPage(page, "/sync/orders-to-pg.html");
  await expectStableScreenshot(page, "sync-definition.png", false);
});

test("overview page @dark", async ({ page }) => {
  await openDocsPage(page, "/index.html");
  await expectStableScreenshot(page, "overview.png");
});
