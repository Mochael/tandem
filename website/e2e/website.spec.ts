import { expect, test } from "@playwright/test"

test("read the docs and follow Markdown links", async ({ page }) => {
	await page.goto("/docs")
	await expect(
		page.getByRole("heading", { name: "Overview", exact: true }),
	).toBeVisible()
	await expect(
		page
			.getByRole("navigation", { name: "Documentation", exact: true })
			.getByRole("link", { name: "Overview", exact: true }),
	).toHaveAttribute("aria-current", "page")
	await expect(
		page.getByLabel("Mermaid diagram", { exact: true }).locator("svg"),
	).toBeVisible()
	await page
		.locator(".markdown")
		.getByRole("link", { name: "Quickstart", exact: true })
		.click()
	await expect(
		page.getByRole("heading", { name: "Quickstart", exact: true }),
	).toBeVisible()
	await expect(page.locator(".shiki").first()).toContainText(
		"pnpm add @tanishqkancharla/tandem-core",
	)
	await expect(
		page
			.locator(".markdown")
			.getByRole("link", { name: "examples/todo", exact: true })
			.first(),
	).toHaveAttribute(
		"href",
		"https://github.com/tanishqkancharla/tandem/tree/main/examples/todo",
	)
	await page
		.locator(".markdown")
		.getByRole("link", { name: "Server", exact: true })
		.first()
		.click()
	await expect(page).toHaveURL(/\/docs\/server#compound-ids$/)
	await expect(
		page.getByRole("heading", { name: "Compound IDs", exact: true }),
	).toBeInViewport()
	await page.goto("/docs/design/relational-queries")
	await expect(page.getByRole("heading", { level: 1 })).toContainText(
		"Relational",
	)
	await page
		.locator(".markdown")
		.getByRole("link", { name: "Queries", exact: true })
		.click()
	await expect(
		page.getByRole("heading", { name: "Queries", exact: true }),
	).toBeVisible()
	await expect(page.getByRole("table").first()).toBeVisible()
	await page.reload()
	await expect(
		page.getByRole("heading", { name: "Queries", exact: true }),
	).toBeVisible()
	await page.goto("/docs/not-a-page")
	await expect(
		page.getByRole("heading", { name: "Page not found" }),
	).toBeVisible()
})

test("find a page and keep the selected theme on a narrow screen", async ({
	page,
}) => {
	await page.setViewportSize({ width: 390, height: 844 })
	await page.goto("/docs")
	await page.getByRole("button", { name: "Menu", exact: true }).click()
	const filter = page.getByRole("searchbox", {
		name: "Filter documentation pages",
	})
	await filter.fill("nothing-matches")
	await expect(
		page.getByText("No pages found. Try another name."),
	).toBeVisible()
	await filter.fill("qUeR")
	const nav = page.getByRole("navigation", {
		name: "Documentation",
		exact: true,
	})
	await expect(nav.getByRole("link")).toHaveCount(2)
	await nav.getByRole("link", { name: "Queries", exact: true }).click()
	await expect(
		page.getByRole("heading", { name: "Queries", exact: true }),
	).toBeVisible()
	await expect(
		page.getByRole("button", { name: "Menu", exact: true }),
	).toHaveAttribute("aria-expanded", "false")
	await page.getByRole("button", { name: "Menu", exact: true }).click()
	await page.getByRole("button", { name: "System Theme" }).click()
	await page.getByRole("option", { name: "Dark", exact: true }).click()
	await expect(page.locator("html")).toHaveAttribute("data-theme", "dark")
	await page.reload()
	await expect(page.locator("html")).toHaveAttribute("data-theme", "dark")
	await expect(
		page.getByRole("heading", { name: "Queries", exact: true }),
	).toBeVisible()
	await expect(page.locator("body")).toHaveJSProperty("scrollWidth", 390)
})
