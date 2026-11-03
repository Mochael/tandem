import "@fontsource-variable/geist"
import { createFileRoute } from "@tanstack/react-router"
import * as errore from "errore"
import { useEffect, useState } from "react"

export const Route = createFileRoute("/")({ component: Home })

const setupPrompt =
	"Follow https://tandemsync.dev/setup.md to add Tandem sync to this project."

class CopyPromptError extends errore.createTaggedError({
	name: "CopyPromptError",
	message: "Could not copy the setup prompt",
}) {}

function Home() {
	const [copy, setCopy] = useState<"idle" | "copied" | "failed">("idle")
	useEffect(() => {
		if (copy === "idle") return
		const timeout = setTimeout(() => setCopy("idle"), 2000)
		return () => clearTimeout(timeout)
	}, [copy])

	async function copyPrompt() {
		const result = await navigator.clipboard
			.writeText(setupPrompt)
			.catch((cause) => new CopyPromptError({ cause }))
		if (result instanceof Error) {
			console.warn(result.message, result.cause)
			setCopy("failed")
			return
		}
		setCopy("copied")
	}

	return (
		<div className="home">
			<header className="home-header">
				<a className="home-brand" href="/">
					<span className="home-mark" aria-hidden="true" />
					Tandem
				</a>
				<nav aria-label="Main" className="home-navigation">
					<a href="https://docs.tandemsync.dev">
						<svg
							aria-hidden="true"
							width="16"
							height="16"
							viewBox="0 0 16 16"
							fill="none"
							stroke="currentColor"
							strokeWidth="1.4"
							strokeLinejoin="round"
						>
							<path d="M2 3.2c1.9-.7 4-.6 6 .8v9.5c-2-1.4-4.1-1.5-6-.8z" />
							<path d="M14 3.2c-1.9-.7-4-.6-6 .8v9.5c2-1.4 4.1-1.5 6-.8z" />
						</svg>
						Docs
					</a>
					<a
						href="https://github.com/tanishqkancharla/tandem"
						aria-label="GitHub"
					>
						{/* GitHub's Octicons mark-github-16 (MIT): https://github.com/primer/octicons */}
						<svg
							aria-hidden="true"
							width="16"
							height="16"
							viewBox="0 0 16 16"
							fill="currentColor"
						>
							<path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z" />
						</svg>
						<span className="home-navigation-label">GitHub</span>
					</a>
				</nav>
			</header>
			<main id="main" className="home-main">
				<h1>
					Real-time sync,
					<br />
					no new infra
				</h1>
				<p>
					Tandem is an open-source sync engine made of two libraries: a local
					database for every client and a library for your server. Writes land
					instantly, sync in the background, and your server stays the source of
					truth.
				</p>
				<div className="home-start">
					<button
						type="button"
						className="home-prompt"
						title={setupPrompt}
						onClick={() => void copyPrompt()}
					>
						Add Tandem to my app
						<span className="home-prompt-icon" aria-hidden="true">
							{copy === "copied" ? (
								<svg
									width="15"
									height="15"
									viewBox="0 0 16 16"
									fill="none"
									stroke="currentColor"
									strokeWidth="1.6"
									strokeLinecap="round"
									strokeLinejoin="round"
								>
									<path d="M3 8.5l3.2 3L13 4.5" />
								</svg>
							) : (
								<svg
									width="15"
									height="15"
									viewBox="0 0 16 16"
									fill="none"
									stroke="currentColor"
									strokeWidth="1.4"
								>
									<rect x="5" y="5" width="8.5" height="8.5" rx="1.8" />
									<path d="M10.5 5V3.8A1.8 1.8 0 0 0 8.7 2H3.8A1.8 1.8 0 0 0 2 3.8v4.9a1.8 1.8 0 0 0 1.8 1.8H5" />
								</svg>
							)}
						</span>
					</button>
					<a className="home-docs-link" href="https://docs.tandemsync.dev">
						Read the docs <span aria-hidden="true">→</span>
					</a>
				</div>
				<p className="home-prompt-status" role="status">
					{copy === "copied"
						? "Copied. Paste it into your coding agent."
						: copy === "failed"
							? "Copy failed. Open tandemsync.dev/setup.md instead."
							: ""}
				</p>
			</main>
		</div>
	)
}
