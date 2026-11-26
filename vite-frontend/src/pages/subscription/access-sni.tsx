import { useEffect, useRef, useState } from "react";

import { getSubscriptionSNI, type SNIView, type SubTrafficRange } from "@/api";
import { useVisiblePolling } from "@/hooks/use-visible-polling";

export function AccessSNI({
  id,
  range,
  start,
}: {
  id: number;
  range: SubTrafficRange;
  start: number;
}) {
  const [data, setData] = useState<SNIView | null>(null);
  const [error, setError] = useState("");
  const refresh = useRef<(() => Promise<void>) | undefined>(undefined);

  useEffect(() => {
    let active = true,
      busy = false;
    const load = async () => {
      if (busy) return;
      busy = true;
      try {
        const res = await getSubscriptionSNI(id, range, start);

        if (!active) return;
        if (res.code !== 0 || !res.data) throw Error(res.msg || "加载失败");
        setData(res.data);
        setError("");
      } catch (e) {
        if (active) setError(e instanceof Error ? e.message : "读取失败");
      } finally {
        busy = false;
      }
    };

    refresh.current = load;
    void load();

    return () => {
      active = false;
      refresh.current = undefined;
    };
  }, [id, range, start]);
  useVisiblePolling(async () => {
    await refresh.current?.();
  }, 30_000);
  if (error)
    return (
      <p className="text-xs text-danger" role="status">
        {error}
      </p>
    );
  if (!data)
    return (
      <p className="text-xs text-default-500" role="status">
        正在读取所选时段…
      </p>
    );

  return (
    <div className="space-y-2 text-xs">
      <p className="text-default-500">
        采集中 {data.ready} · 已停止 {data.stopped} · 未就绪 {data.disabled} ·
        节点不可用或版本过旧 {data.unavailable}
      </p>
      {data.items.length ? (
        <table className="w-full text-left">
          <thead>
            <tr>
              <th>可见 SNI</th>
              <th className="text-right">握手次数</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((h) => (
              <tr key={h.domain} className="border-t border-divider">
                <td className="break-all py-1.5 font-mono">{h.domain}</td>
                <td className="text-right tabular-nums">{h.hits}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : (
        <p>
          {data.ready + data.stopped
            ? "此时段没有可显示的 SNI 记录。"
            : data.disabled
              ? "采集器尚未就绪：请检查节点版本、订阅出站标记、运行依赖和 nftables 权限。"
              : data.unavailable
                ? "无法读取节点统计。"
                : "没有可统计的自建节点。"}
        </p>
      )}
      {data.limited && <p>仅展示部分域名及其可汇总次数。</p>}
      <p className="text-default-400">
        Agent 自动采集，按所选时段统计可读取的自建节点上可见的 TLS
        握手。同一网页可能产生多次握手。
      </p>
    </div>
  );
}
