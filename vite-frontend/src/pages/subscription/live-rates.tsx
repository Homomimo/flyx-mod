import axios from "axios";
import { useEffect, useState } from "react";

import { IS_DEMO } from "@/demo";
import { getToken } from "@/utils/session";
import { formatTraffic } from "@/utils/traffic";

type Rate = { up: number; down: number };
type Rates = Record<number, Rate>;

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
        const res = await fetch(`${base}subscription/rates`, {
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

export function RateLabel({ rate }: { rate?: Rate }) {
  return (
    <div
      className="text-xs tabular-nums text-default-500"
      title="约 1 秒采样，仅汇总有有效样本的直连 Agent；外部节点、旧 Agent 或数据不足时不计入。"
    >
      实时速率 ·{" "}
      {rate
        ? `↑ ${formatTraffic(rate.up)}/s　↓ ${formatTraffic(rate.down)}/s`
        : "—"}
    </div>
  );
}
