"""区域额度消息语义的随机化模拟（契约 C2 第 7 节、HubControl BudgetUpdate / GrantSettled）。

不是 ENGR-006 的完整模型：只验证一个后端、多个 Edge Hub 实例、单一租户在
“签发 / 结算 / 账本入账 / 补充 / 余额被动减少 / BudgetUpdate 乱序、重复、过期”下的消息语义。

运行：python3 budget_sim.py            （默认 2,000 个随机场景）
对照：python3 budget_sim.py --delta    （改用“增量确认”语义，预期出现违例）
"""
import math
import random
import sys
from fractions import Fraction

COEF_CHOICES = [1000, 1500, 2000, 3500]  # 路径系数（千分位）


def ceil_billing(raw, coef):
    return -(-raw * coef // 1000)


class Backend:
    def __init__(self, hubs, balance):
        self.balance = Fraction(balance)        # 租户余额（计费字节，精确值，含 MTR-002 余量）
        self.delegated = {h: 0 for h in hubs}
        self.acked = {h: 0 for h in hubs}       # settled_acked_total
        self.version = {h: 0 for h in hubs}
        self.applied = {h: set() for h in hubs}  # 已入账的 (hub, seq)
        self.written_off = Fraction(0)            # 核销的超额消费

    def undelegated(self):
        return self.balance - sum(self.delegated.values())

    def update(self, h):
        self.version[h] += 1
        return ("budget", h, self.version[h], self.delegated[h], self.acked[h])

    def topup(self, h, amount):
        amount = min(amount, max(0, math.floor(self.undelegated())))
        if amount <= 0:
            return None
        self.delegated[h] += amount
        return self.update(h)

    def ingest_settle(self, h, seq, used_raw, coef, settled_billing):
        if seq in self.applied[h]:          # (hub_id, seq) 幂等
            return None
        self.applied[h].add(seq)
        # 评审决定 H4：结算中超出该实例剩余委托的部分是 MTR-013 允许的超额消费，在来源处核销、不追缴，
        # 不得占用委托给其他实例的余额。正常运行时 Edge Hub 只在委托内签发，此上限不会触发。
        exact = Fraction(used_raw * coef, 1000)
        covered = min(exact, Fraction(self.delegated[h]))
        self.written_off += exact - covered
        self.balance -= covered
        # 区域额度按约定的 ceil 值下调，不低于 0；累计值照常增加，保证 Edge Hub 的 pending 归零
        self.delegated[h] -= min(settled_billing, self.delegated[h])
        self.acked[h] += settled_billing
        return self.update(h)

    def passive_reduce(self, amount):
        """MTR-013：余额被动减少后下调在线实例的 delegated，使 Σ delegated ≤ 余额。"""
        self.balance -= min(Fraction(amount), self.balance)   # 作废的额度不会超过现有余额
        msgs = []
        over = sum(self.delegated.values()) - self.balance
        for h in sorted(self.delegated):
            if over <= 0:
                break
            cut = min(self.delegated[h], math.ceil(over))
            if cut > 0:
                self.delegated[h] -= cut
                over -= cut
                msgs.append(self.update(h))
        return msgs


class Hub:
    def __init__(self, name, delta_mode=False):
        self.name = name
        self.delta_mode = delta_mode
        self.version = 0
        self.delegated = 0
        self.acked = 0                 # 本地记录的 settled_acked_total
        self.settled_total = 0         # settled_local_total
        self.issued = 0                # 已签发未结算的预留（计费字节）
        self.grants = {}               # grant_id -> (raw, coef, reserved)
        self.seq = 0
        self.next_grant = 0

    def pending(self):
        return self.settled_total - self.acked

    def available(self):
        return max(0, self.delegated - self.issued - self.pending())

    def apply_budget(self, version, delegated, acked_total):
        if self.delta_mode:
            # 对照组：把每条更新当作“增量”应用，且不按版本去重
            self.delegated = delegated
            self.acked += acked_total - getattr(self, "_last_seen", 0)
            self._last_seen = acked_total
            return
        if version <= self.version:    # 只应用版本更大的更新
            return
        self.version = version
        self.acked = acked_total       # ① 记录累计值
        self.delegated = delegated     # ②③ available 由 pending() 派生

    def issue(self, rng):
        coef = rng.choice(COEF_CHOICES)
        raw = rng.randint(1, 50_000)
        reserved = ceil_billing(raw, coef)
        if reserved > self.available():
            return
        gid = self.next_grant
        self.next_grant += 1
        self.grants[gid] = (raw, coef, reserved)
        self.issued += reserved

    def settle(self, rng):
        if not self.grants:
            return None
        gid = rng.choice(list(self.grants))
        raw, coef, reserved = self.grants.pop(gid)
        used = rng.randint(0, raw)
        s = ceil_billing(used, coef)
        self.issued -= reserved
        self.settled_total += s
        self.seq += 1
        return ("settle", self.name, self.seq, used, coef, s)


def run(seed, delta_mode=False):
    rng = random.Random(seed)
    hubs = ["hub-a", "hub-b", "hub-c"]
    be = Backend(hubs, balance=rng.randint(2_000_000, 20_000_000))
    hs = {h: Hub(h, delta_mode) for h in hubs}
    to_hub, to_be = [], []
    passive = False
    violations = []

    def check(tag):
        # I0：余额与区域额度不为负（uint64 不得下溢）
        if be.balance < 0 or min(be.delegated.values()) < 0:
            violations.append(f"{tag}: negative balance or delegated")
        # I1：后端不变量 Σ delegated ≤ 租户余额（MTR-004）
        if sum(be.delegated.values()) > be.balance:
            violations.append(f"{tag}: Σdelegated {sum(be.delegated.values())} > balance {float(be.balance):.1f}")
        for h in hs.values():
            # I2：待入账结算不为负（没有多扣）
            if h.pending() < 0:
                violations.append(f"{tag}: {h.name} settled_pending < 0 ({h.pending()})")

    for step in range(rng.randint(200, 800)):
        op = rng.random()
        h = hs[rng.choice(hubs)]
        if op < 0.25:
            m = be.topup(h.name, rng.randint(10_000, 400_000))
            if m:
                to_hub.append(m)
        elif op < 0.50:
            h.issue(rng)
        elif op < 0.70:
            m = h.settle(rng)
            if m:
                to_be.append(m)
                if rng.random() < 0.3:
                    to_be.append(m)                 # 重复发送
        elif op < 0.72:
            to_hub.extend(be.passive_reduce(rng.randint(10_000, 300_000)))
            passive = True
        else:
            # 投递：随机挑选消息（乱序），有概率重复投递
            if to_be and rng.random() < 0.5:
                m = to_be.pop(rng.randrange(len(to_be)))
                _, hn, seq, used, coef, s = m
                r = be.ingest_settle(hn, seq, used, coef, s)
                if r:
                    to_hub.append(r)
            elif to_hub:
                m = to_hub.pop(rng.randrange(len(to_hub)))
                _, hn, ver, dl, ack = m
                hs[hn].apply_budget(ver, dl, ack)
                if rng.random() < 0.2:
                    hs[hn].apply_budget(ver, dl, ack)   # 重复投递
        check(f"seed {seed} step {step}")

    # 静止：结算所有授权并投递全部消息
    for h in hs.values():
        while h.grants:
            to_be.append(h.settle(rng))
    while to_be or to_hub:
        if to_be:
            _, hn, seq, used, coef, s = to_be.pop(rng.randrange(len(to_be)))
            r = be.ingest_settle(hn, seq, used, coef, s)
            if r:
                to_hub.append(r)
        if to_hub:
            _, hn, ver, dl, ack = to_hub.pop(rng.randrange(len(to_hub)))
            hs[hn].apply_budget(ver, dl, ack)
    check(f"seed {seed} quiescent")
    for h in hs.values():
        # I3：静止后双方账目逐字节一致
        if h.acked != be.acked[h.name] or h.delegated != be.delegated[h.name] or h.pending() != 0:
            violations.append(f"seed {seed} quiescent: {h.name} hub(delegated={h.delegated}, acked={h.acked}) "
                              f"!= backend(delegated={be.delegated[h.name]}, acked={be.acked[h.name]})")
    return violations, passive, be.written_off


if __name__ == "__main__":
    delta = "--delta" in sys.argv
    n = 2000
    bad, with_passive, wrote_off = 0, 0, 0
    first = []
    for seed in range(n):
        v, p, wo = run(seed, delta)
        with_passive += p
        wrote_off += wo > 0
        if v:
            bad += 1
            first.extend(v[:1])
    mode = "增量确认（对照组）" if delta else "累计确认（契约语义）"
    print(f"{mode}：{n} 个场景，其中 {with_passive} 个含余额被动减少、{wrote_off} 个出现超额核销；违例场景 {bad} 个")
    for line in first[:3]:
        print("  例：", line)
    sys.exit(0 if (bad == 0) != delta else 1)
