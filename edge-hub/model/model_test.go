package model

import (
	"math/big"
	"math/rand"
	"testing"
)

func TestCeilBillingMatchesBigInt(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	cases := [][2]uint64{{0, 1000}, {1, 1000}, {1, 1500}, {3, 333}, {1 << 62, 3500}, {^uint64(0) / 4, 4000}}
	for i := 0; i < 20000; i++ {
		cases = append(cases, [2]uint64{rng.Uint64() >> uint(rng.Intn(64)), uint64(rng.Intn(10000))})
	}
	for _, c := range cases {
		want := new(big.Int).Mul(new(big.Int).SetUint64(c[0]), new(big.Int).SetUint64(c[1]))
		want.Add(want, big.NewInt(999))
		want.Div(want, big.NewInt(1000))
		if c[0] == 0 || c[1] == 0 {
			want.SetUint64(0)
		}
		if !want.IsUint64() {
			continue
		}
		if got := CeilBilling(c[0], c[1]); got != want.Uint64() {
			t.Fatalf("CeilBilling(%d, %d) = %d, want %s", c[0], c[1], got, want)
		}
	}
}
