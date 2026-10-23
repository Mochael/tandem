export {
	executeQueryAsync,
	executeQuerySync,
	executeScanWindowAsync,
	executeScanWindowSync,
	InvalidScanWindowError,
} from "./query/executeQuery.js"
export type { ScanWindowRecord } from "./query/executeQuery.js"
export { collectionIdsEqual, collectionIdToTuple } from "./schema/Schema.js"
// Tab coordination runs automatically in browsers. These are for tests and
// for hosts outside a browser document, such as workers.
export {
	browserTabEnvironment,
	InMemoryTab,
	InMemoryTabs,
} from "./tabs/TabEnvironment.js"
export type {
	TabChannelApi,
	TabEnvironmentApi,
	TabLocksApi,
	TabMessageListener,
} from "./tabs/TabEnvironment.js"
