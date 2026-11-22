import axios from "axios";
import { useEffect, useState } from "react";

import { IS_DEMO } from "@/demo";
import { getToken } from "@/utils/session";
import { formatTraffic } from "@/utils/traffic";

type Pair = { up: number; down: number };
type Rate = {
  instant: Pair | null;
  average: Pair | null;
  valid: number;
  averageValid: number;
  expected: number;
};
// Older panels ignore ?v=2 and return bare pairs. Don't invent coverage.
type WireRate = Rate | Pair;
type Rates = Record<number, WireRate>;

export function useSubscriptionRates(enabled: boolean): Rates {
  const [rates, setRates] = useState<Rates>({});

  useEffect(() => {
    if (!enabled || IS_DEMO) return;
    let disposed = false;
    let active: AbortController | undefined;
    let retry: ReturnType<typeof setTimeout>;
    const visible = () =>
      document.visibilityState !== "hidden" && navigator.onLine !== false;
    const connect = async () => {
      if (disposed || active || !visible()) return;
      clearTimeout(retry);
      const controller = new AbortController();

      active = controller;
      // A silently stalled proxy must not leave an old speed on screen.
      let watchdog = setTimeout(() => controller.abort(), 5000);

      try {
        const base = (axios.defaults.baseURL || "/api/v1/").replace(
          /\/?$/,
          "/",
        );
        const res = await fetch(`${base}subscription/rates?v=2`, {
          method: "POST",
          headers: { Authorization: getToken() || "" },
          signal: controller.signal,
        });

        if (
          !res.ok ||
          !res.body ||
          !res.headers.get("content-type")?.includes("application/x-ndjson")
        )
          return;
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (!controller.signal.aborted) {
          const { done, value } = await reader.read();

          if (done || controller.signal.aborted) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");

          buffer = lines.pop() || "";
          if (lines.length) {
            setRates(JSON.parse(lines[lines.length - 1]) as Rates);
            clearTimeout(watchdog);
            watchdog = setTimeout(() => controller.abort(), 4000);
          }
        }
      } catch {
        // Old panels, disconnects and unsupported agents show an unavailable rate.
      } finally {
        clearTimeout(watchdog);
        controller.abort();
        if (active === controller) {
          active = undefined;
          if (!disposed) {
            setRates({});
            if (visible()) retry = setTimeout(connect, 1000);
          }
        }
      }
    };
    const stop = () => {
      clearTimeout(retry);
      active?.abort();
      active = undefined;
    };
    const sync = () => {
      if (visible()) void connect();
      else {
        stop();
        setRates({});
      }
    };

    setRates({});
    sync();
    document.addEventListener("visibilitychange", sync);
    window.addEventListener("online", sync);
    window.addEventListener("offline", sync);

    return () => {
      disposed = true;
      stop();
      document.removeEventListener("visibilitychange", sync);
      window.removeEventListener("online", sync);
      window.removeEventListener("offline", sync);
    };
  }, [enabled]);

  return enabled ? rates : {};
}

function speed(pair?: Pair | null) {
  if (!pair || !Number.isFinite(pair.up) || !Number.isFinite(pair.down))
    return "—";

  const fmt = (value: number) =>
    value > 0 && value < 1 ? "<1 B" : formatTraffic(value);

  return `↑ ${fmt(pair.up)}/s　↓ ${fmt(pair.down)}/s`;
}
function coverage(valid: number, expected: number) {
  if (!expected) return "无可计量节点";

  return `${valid}/${expected} · ${valid === expected ? "完整" : valid ? "部分" : "暂无样本"}`;
}
export function RateLabel({
  rate,
  details = false,
}: {
  rate?: WireRate;
  details?: boolean;
}) {
  const measured = rate && "instant" in rate ? rate : undefined;
  const instant = rate && "up" in rate ? rate : measured?.instant;

  return (
    <div
      className="text-xs tabular-nums text-default-500"
      title="↑ 上传、↓ 下载；仅汇总当前分发订阅有有效样本的直连节点。0 B/s 表示已测得空闲，— 表示无有效样本。"
    >
      <div>
        实时速率 · {speed(instant)}
        {measured && `　${coverage(measured.valid, measured.expected)}`}
      </div>
      {details && (
        <div>
          近 3 秒均速 · {speed(measured?.average)}
          {measured &&
            `　${coverage(measured.averageValid, measured.expected)}`}
        </div>
      )}
    </div>
  );
}
