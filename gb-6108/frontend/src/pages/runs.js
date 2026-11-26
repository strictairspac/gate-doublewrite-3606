import { api } from "../api/workbench.js";
import { empty, pageHeading, section } from "../components/common/panel.js";
import { status } from "../components/common/status.js";
import { table } from "../components/common/table.js";
import { poll } from "../hooks/poll.js";
import { el } from "../utils/dom.js";
import { time } from "../utils/format.js";

const ACTIVE = new Set(["queued", "running"]);

function progress(run) {
  const total = run.total_pages ?? 0;
  const landed = run.landed_pages ?? 0;
  const pct = total ? Math.round((landed / total) * 100) : 0;
  return el("div", {class: "progress-track", "data-testid": "run-progress"},
    el("div", {class: "progress-bar", role: "progressbar", "aria-valuemin": 0, "aria-valuemax": total || 0,
      "aria-valuenow": landed, "aria-label": "已落库页进度"},
      el("span", {style: `width:${pct}%`})),
    el("span", {class: "progress-meta"}, total ? `已落库 ${landed} / ${total} 页（${pct}%）` : "等待领取批次…"));
}

function handoff(run) {
  if (!run.handed_off) return null;
  const finished = run.status === "done" || run.status === "failed";
  return el("div", {class: "handoff-note", "data-testid": "handoff"},
    el("strong", {}, finished ? "原批次已接手并续跑完成" : "原批次已被接手，正在续跑"),
    el("span", {}, `原处理进程：${run.last_owner_id ?? "—"}　当前进程：${run.owner_id ?? "—"}`),
    el("span", {class: "progress-meta"}, `累计领取 ${run.attempts} 次 · 已落库页不重复计算，从断点继续`));
}

function reportState(run) {
  const active = ACTIVE.has(run.status);
  const message = run.status === "queued"
    ? "批次已提交，等待 worker 领取，页面会自动更新。"
    : run.status === "running"
      ? "巡检按页落库；即使处理进程失联，另一进程也会接手续跑，本页自动刷新。"
      : run.failure || run.result?.summary;
  return el("div", {class: "stack", id: "run-state"},
    status(run.status),
    handoff(run),
    run.status === "running" ? progress(run) : null,
    el("p", {}, message),
    run.status === "done" ? el("a", {class: "button", href: `#evidence/${run.id}`}, "查看业务证据") : null);
}

export async function runsPage(config, route, signal) {
  const items = await api.runs();
  const latest = route.id ? await api.run(route.id, signal) : items[0];
  const detail = latest ? reportState(latest) : empty("进入资料页，选用一个业务样本并提交执行。");
  async function refresh() {
    try {
      const next = await api.run(latest.id, signal);
      if (!signal.aborted) {
        detail.replaceChildren(...reportState(next).childNodes);
        const badge = document.querySelector(`[data-run="${next.id}"] .status`);
        if (badge) badge.replaceWith(status(next.status));
      }
      return ACTIVE.has(next.status);
    } catch (error) {
      if (!signal.aborted) detail.append(el("p", {role: "alert"}, error.message));
      return false;
    }
  }
  if (latest && ACTIVE.has(latest.status)) poll(refresh, signal);
  return el("div", {class: "page"},
    pageHeading(config.modules[2], config.parameter_label),
    latest ? section(latest.case_title, detail) : detail,
    section("执行批次", items.length ? table(["资料", "编号", "参数", "状态", "进度", "提交时间"],
      items.map(run => [el("a", {"data-run": run.id, href: `#runs/${run.id}`}, run.case_title), run.id,
        run.parameter, el("span", {"data-run": run.id}, status(run.status)),
        run.total_pages ? `${run.landed_pages}/${run.total_pages}` : "—", time(run.created_at)]), "执行批次") :
      empty("暂无执行记录。")));
}
