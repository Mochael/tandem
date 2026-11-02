import { expect, test } from "@playwright/test"

function uniqueTodoText(label: string) {
	return `${label} ${Date.now()}-${crypto.randomUUID()}`
}

test("a browser catches changes it missed once its event stream reconnects", async ({
	baseURL,
	browser,
	page: writer,
}) => {
	await writer.goto("/")
	const readerContext = await browser.newContext({ baseURL })
	const reader = await readerContext.newPage()

	// The reader loads while its event stream cannot connect.
	const eventStream = "**/api/tandem/events*"
	await readerContext.route(eventStream, (route) =>
		route.abort("connectionrefused"),
	)
	await reader.goto("/")
	await reader
		.getByRole("checkbox", { name: "Build something with Tandem" })
		.waitFor()
	const todoText = uniqueTodoText("Missed while disconnected")
	await writer.getByLabel("New todo").fill(todoText)
	await writer.getByRole("button", { name: "Add" }).click()
	await expect(writer.getByRole("checkbox", { name: todoText })).toBeVisible()

	// The stream's retry pulls the missed todo without another write.
	await readerContext.unroute(eventStream)
	await expect(reader.getByRole("checkbox", { name: todoText })).toBeVisible({
		timeout: 15_000,
	})

	// The reconnected stream delivers later pokes.
	await writer.getByRole("button", { name: `Delete ${todoText}` }).click()
	await expect(reader.getByRole("checkbox", { name: todoText })).toHaveCount(0)
	await readerContext.close()
})

test("a browser without an event stream catches up on the fallback pull", async ({
	baseURL,
	browser,
	page: writer,
}) => {
	await writer.goto("/")
	const readerContext = await browser.newContext({ baseURL })
	await readerContext.route("**/api/tandem/events*", (route) =>
		route.abort("connectionrefused"),
	)
	const reader = await readerContext.newPage()
	await reader.clock.install()
	await reader.goto("/")
	await reader
		.getByRole("checkbox", { name: "Build something with Tandem" })
		.waitFor()

	// No poke reaches the reader, so the new todo waits for the fallback pull.
	const todoText = uniqueTodoText("Fallback todo")
	await writer.getByLabel("New todo").fill(todoText)
	await writer.getByRole("button", { name: "Add" }).click()
	await expect(writer.getByRole("checkbox", { name: todoText })).toBeVisible()
	await reader.clock.fastForward(60_000)
	await expect(reader.getByRole("checkbox", { name: todoText })).toBeVisible()

	await writer.getByRole("button", { name: `Delete ${todoText}` }).click()
	await expect(async () => {
		await reader.clock.fastForward(60_000)
		await expect(reader.getByRole("checkbox", { name: todoText })).toHaveCount(
			0,
			{ timeout: 500 },
		)
	}).toPass()
	await readerContext.close()
})
