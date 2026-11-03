import {
	ClientOnly,
	HeadContent,
	Link,
	Outlet,
	Scripts,
	createRootRoute,
} from "@tanstack/react-router"
import { MauiProvider, themeFoucScript } from "maui"
import type { ReactNode } from "react"
import "../styles.css"

export const Route = createRootRoute({
	head: () => ({
		meta: [
			{ charSet: "utf-8" },
			{ name: "viewport", content: "width=device-width, initial-scale=1" },
			{ title: "Tandem — Real-time sync, no new infra" },
			{
				name: "description",
				content:
					"Tandem is an open-source sync engine made of two libraries: a local database for every client and a library for your server.",
			},
		],
		links: [{ rel: "icon", type: "image/svg+xml", href: "/favicon.svg" }],
	}),
	component: Application,
	shellComponent: RootDocument,
	notFoundComponent: () => (
		<main id="main" className="not-found">
			<h1>Page not found</h1>
			<p>That page doesn’t exist. Find your way back to Tandem.</p>
			<Link to="/">Home</Link>
			{" · "}
			<Link to="/docs">Documentation</Link>
		</main>
	),
})

function Application() {
	// Maui's theme and scale providers read matchMedia during their first render.
	return (
		<ClientOnly fallback={<p className="loading">Loading Tandem…</p>}>
			<MauiProvider>
				<a className="skip-link" href="#main">
					Skip to content
				</a>
				<Outlet />
			</MauiProvider>
		</ClientOnly>
	)
}

function RootDocument({ children }: { children: ReactNode }) {
	return (
		<html lang="en" suppressHydrationWarning>
			<head>
				<script dangerouslySetInnerHTML={{ __html: themeFoucScript }} />
				<HeadContent />
			</head>
			<body>
				{children}
				<Scripts />
			</body>
		</html>
	)
}
