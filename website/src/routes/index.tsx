import { createFileRoute } from "@tanstack/react-router"
import * as errore from "errore"
import { Button, P, Prose, Text, iconSizeValues } from "maui"
import { useEffect, useState } from "react"

export const Route = createFileRoute("/")({ component: Home })

class GithubStarsError extends errore.createTaggedError({
	name: "GithubStarsError",
	message: "Could not load GitHub stars: $reason",
}) {}

async function loadStars() {
	const response = await fetch(
		"https://api.github.com/repos/tanishqkancharla/tandem",
	).catch((cause) => new GithubStarsError({ reason: "Request failed", cause }))
	if (response instanceof Error) return response
	if (!response.ok)
		return new GithubStarsError({ reason: `HTTP ${response.status}` })
	const repository = await (
		response.json() as Promise<{ stargazers_count: number }>
	).catch(
		(cause) => new GithubStarsError({ reason: "Invalid response", cause }),
	)
	if (repository instanceof Error) return repository
	return repository.stargazers_count
}

function Home() {
	const [stars, setStars] = useState<number>()
	useEffect(() => {
		void loadStars().then((result) => {
			if (result instanceof Error) {
				console.warn(result.message)
				return
			}
			setStars(result)
		})
	}, [])
	return (
		<div className="home">
			<header className="home-header">
				<Text size="lg" fontWeight={600}>
					Tandem
				</Text>
				<nav aria-label="Main" className="home-navigation">
					<Button
						variant="quiet"
						aria-label={
							stars === undefined
								? "GitHub repository"
								: `GitHub repository, ${stars} stars`
						}
						onPress={() =>
							window.location.assign(
								"https://github.com/tanishqkancharla/tandem",
							)
						}
					>
						{/* GitHub's Octicons mark-github-16 (MIT): https://github.com/primer/octicons */}
						<svg
							aria-hidden="true"
							focusable="false"
							width={iconSizeValues.sm}
							height={iconSizeValues.sm}
							viewBox="0 0 16 16"
							fill="currentColor"
						>
							<path d="M6.766 11.328c-2.063-.25-3.516-1.734-3.516-3.656 0-.781.281-1.625.75-2.188-.203-.515-.172-1.609.063-2.062.625-.078 1.468.25 1.968.703.594-.187 1.219-.281 1.985-.281.765 0 1.39.094 1.953.265.484-.437 1.344-.765 1.969-.687.218.422.25 1.515.046 2.047.5.593.766 1.39.766 2.203 0 1.922-1.453 3.375-3.547 3.64.531.344.89 1.094.89 1.954v1.625c0 .468.391.734.86.547C13.781 14.359 16 11.53 16 8.03 16 3.61 12.406 0 7.984 0 3.563 0 0 3.61 0 8.031a7.88 7.88 0 0 0 5.172 7.422c.422.156.828-.125.828-.547v-1.25c-.219.094-.5.156-.75.156-1.031 0-1.64-.562-2.078-1.609-.172-.422-.36-.672-.719-.719-.187-.015-.25-.093-.25-.187 0-.188.313-.328.625-.328.453 0 .844.281 1.25.86.313.452.64.655 1.031.655s.641-.14 1-.5c.266-.265.47-.5.657-.656" />
						</svg>
						{stars === undefined ? "—" : stars.toLocaleString("en-US")}
					</Button>
					<Button
						onPress={() =>
							window.location.assign("https://docs.tandemsync.dev")
						}
					>
						Docs
					</Button>
				</nav>
			</header>
			<main id="main" className="home-main">
				<article aria-label="About Tandem">
					<Prose>
						<P>A sync engine and database for building collaborative apps.</P>
					</Prose>
					<Button
						variant="primary"
						variantColor="teal"
						onPress={() =>
							window.location.assign("https://docs.tandemsync.dev/quickstart")
						}
					>
						Get started
					</Button>
				</article>
			</main>
		</div>
	)
}
