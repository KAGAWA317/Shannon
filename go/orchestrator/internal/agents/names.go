package agents

import "hash/fnv"

// Reserved agent index ranges to avoid collision:
// 0-49:    Main subtask agents (parallel/sequential/hybrid execution)
// 100-149: Domain prefetch agents
// 200:     Final synthesis agent
// 201:     Intermediate synthesis agent
// 210:     Research refiner
// 211:     Coverage evaluator
// 212:     Subquery generator
// 213:     Fact extraction
// 214:     Entity localization
// 215:     Fallback search
// 220:     Ads keyword extraction
// 221-229: Ads LP analysis (221+i)
// 230:     Ads synthesis
// 231:     Ads Yahoo JP discover
// 232:     Ads Meta Ad Library discover
// 233:     Ads video analysis
// 240:     React synthesizer
// 250:     DAG synthesis
// 251:     Supervisor synthesis
// 252:     Streaming synthesis
// 260-269: Sagasu competitor snapshot agents (260+i)
// 270:     Sagasu research agent
// 271:     Sagasu synthesis agent
// 272:     Sagasu ads monitor agent
// 300+:    Swarm dynamically spawned agents (300+i)
// 保留的 agent 索引区间（用于避免冲突）：
// 0-49：主子 task agent（并行/顺序/混合执行）
// 100-149：领域预取 agent
// 200：最终汇总 agent；201：中间汇总 agent
// 210-215：研究精炼 / 覆盖评估 / 子查询生成 / 事实抽取 / 实体定位 / 兜底搜索
// 220-229：广告关键词抽取与落地页分析（221+i）
// 230-233：广告汇总 / Yahoo JP 发现 / Meta Ad Library 发现 / 视频分析
// 240-252：React 汇总 / DAG 汇总 / Supervisor 汇总 / streaming 汇总
// 260-269：Sagasu 竞品快照 agent（260+i）
// 270-272：Sagasu 研究 / 汇总 / 广告监控 agent
// 300+：swarm 动态派生的 agent（300+i）
const (
	IdxSynthesis             = 200
	IdxIntermediateSynthesis = 201
	IdxResearchRefiner       = 210
	IdxCoverageEvaluator     = 211
	IdxSubqueryGenerator     = 212
	IdxFactExtraction        = 213
	IdxEntityLocalization    = 214
	IdxFallbackSearch        = 215
	IdxAdsKeywordExtraction  = 220
	IdxAdsLPAnalysisBase     = 221 // Use 221+i for LP analysis
	IdxAdsSynthesis          = 230
	IdxAdsYahooJPDiscover    = 231
	IdxAdsMetaDiscover       = 232
	IdxAdsVideoAnalysis      = 233
	IdxReactSynthesizer      = 240
	IdxDAGSynthesis          = 250
	IdxSupervisorSynthesis   = 251
	IdxStreamingSynthesis    = 252
	IdxDomainPrefetchBase    = 100 // Use 100+i for domain prefetch
	IdxSwarmDynamicBase = 300 // Use 300+i for dynamically spawned swarm agents
)

// stationNames is the pool of Japanese station-inspired agent names.
// The list is fixed to maintain determinism for workflow replays.
// stationNames 是受日本车站名启发的 agent 名称池。
// 列表固定不变，以保证 workflow 重放时的确定性。
var stationNames = []string{
	// Classics with proper romanization
	"Ōme", "Gora", "Maji", "Ebisu", "Ōsaki",
	"Otaru", "Namba", "Tenma", "Mejiro", "Kōenji",
	"Gotanda", "Ryōgoku", "Yūtenji", "Nippori", "Asagaya",
	"Mojikō", "Kottoi", "Taishō", "Yumoto", "Odawara",
	"Enoshima", "Ogikubo", "Ichigaya", "Komazawa", "Todoroki",
	// Quirky names
	"Obama", "Usa", "Gero", "Ōboke", "Koboke",
	"Naruto", "Zushi", "Fussa", "Oppama", "Pippu",
	"Mashike", "Zōshiki",
	// Remote & scenic gems
	"Nikkō", "Hakone", "Beppu", "Atami", "Wakkanai",
	"Koboro", "Shimonada", "Tadami", "Tsuwano", "Okutama",
	"Nagatoro", "Kazamatsuri", "Chōshi", "Kururi", "Biei",
	"Minobu", "Shimonita",
	// Saitama & West Tokyo deep cuts
	"Tama", "Musashi", "Urawa", "Kawagoe", "Hannō",
	"Chichibu", "Takao", "Mitaka", "Kichijōji",
	// Bonus obscure finds
	"Karasuyama", "Ashikaga", "Sasago", "Shimokita", "Kuragano",
}

// GetAgentName returns a deterministic agent name for a given workflow and index.
// This is safe for Temporal workflow replays: the same workflowID and index
// will always produce the same name.
// GetAgentName 按 workflow 与索引返回确定性的 agent 名称。
// 这对 Temporal workflow 重放是安全的：相同的 workflowID 与索引总会得到相同的名称。
func GetAgentName(workflowID string, index int) string {
	if len(stationNames) == 0 {
		return ""
	}

	hash := fnv32a(workflowID)
	nameIndex := (int(hash) + index) % len(stationNames)
	return stationNames[nameIndex]
}

func fnv32a(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
