use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Formatter};
use std::ops::{Add, Sub};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub(crate) enum Side {
    #[serde(rename = "A")]
    Ask,
    #[serde(rename = "B")]
    Bid,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Oid(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Px(u64);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Sz(u64);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Coin(String);

impl Sz {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(super) const fn is_positive(self) -> bool {
        self.0 > 0
    }
    pub(super) const fn is_zero(self) -> bool {
        self.0 == 0
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
    pub(crate) const fn decrement_sz(&mut self, dec: u64) {
        self.0 = self.0.saturating_sub(dec);
    }
}

impl Px {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
    pub(crate) const fn value(self) -> u64 {
        self.0
    }
}

impl Oid {
    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }
}

pub(crate) trait InnerOrder: Clone {
    fn coin(&self) -> Coin;
    fn oid(&self) -> Oid;
    fn side(&self) -> Side;
    fn limit_px(&self) -> Px;
    fn sz(&self) -> Sz;
    fn decrement_sz(&mut self, dec: Sz);
    fn fill(&mut self, maker_order: &mut Self) -> Sz;
    fn modify_sz(&mut self, sz: Sz);
    fn convert_trigger(&mut self, ts: u64);
}

impl Coin {
    pub(crate) fn new(coin: &str) -> Self {
        Self(coin.to_string())
    }

    pub(crate) fn value(&self) -> String {
        self.0.clone()
    }

    pub(crate) fn is_spot(&self) -> bool {
        // HyperLiquid spot markets are encoded as `@<n>` (legacy spot index),
        // `#<n>` (newer spot index used for assets like #1180), or the special
        // `PURR/USDC` ticker. Missing the `#` prefix here previously caused
        // spot diffs to fall through `ignore_spot` filtering and crash the
        // listener with "Unable to find order on the book" when the book had
        // not yet been grafted via absorb_extra_books.
        self.0.starts_with('@') || self.0.starts_with('#') || self.0 == "PURR/USDC"
    }
}

impl Add<Self> for Sz {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        Self(self.0 + rhs.0)
    }
}

impl Sub<Self> for Sz {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self(self.0 - rhs.0)
    }
}

// Multiply all sizes and prices by 10^MAX_DECIMALS for ease of computation.
const MULTIPLIER: f64 = 100_000_000.0;
const MULTIPLIER_INT: u64 = 100_000_000;

// Decimal string of a fixed-point value, trailing fractional zeros trimmed.
//
// Integer arithmetic instead of format!("{:.8}", value as f64 / MULTIPLIER):
// that float formatting was ~15% of obs CPU (it runs for every level of every
// L2 book sent to every client), and it is only exact while the float
// round-trip is -- below ~4.5e7 whole units. Output is identical in that range
// (see tests) and exact above it.
fn fixed_to_str(value: u64) -> String {
    let (int, frac) = (value / MULTIPLIER_INT, value % MULTIPLIER_INT);
    if frac == 0 {
        return int.to_string();
    }
    let mut s = format!("{int}.{frac:08}");
    s.truncate(s.trim_end_matches('0').len());
    s
}

impl Debug for Px {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / MULTIPLIER))
    }
}

impl Debug for Sz {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (self.value() as f64 / MULTIPLIER))
    }
}

impl Px {
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn parse_from_str(value: &str) -> Result<Self> {
        let value = (value.parse::<f64>()? * MULTIPLIER).round() as u64;
        Ok(Self::new(value))
    }

    #[must_use]
    pub(crate) fn to_str(self) -> String {
        fixed_to_str(self.value())
    }

    // Integer log: same result as the old floor(log10(v as f64)) + 1 wherever
    // that float was exact, without the libm call on every L2 bucket.
    pub(crate) fn num_digits(self) -> u32 {
        self.value().checked_ilog10().map_or(1, |d| d + 1)
    }
}

impl Sz {
    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    pub(crate) fn parse_from_str(value: &str) -> Result<Self> {
        let value = (value.parse::<f64>()? * MULTIPLIER).round() as u64;
        Ok(Self::new(value))
    }

    #[must_use]
    pub(crate) fn to_str(self) -> String {
        fixed_to_str(self.value())
    }
}

#[cfg(test)]
mod tests {
    use super::{MULTIPLIER, Px, Sz, fixed_to_str};
    use rand::Rng;

    // The implementations these replaced, kept as the reference.
    fn old_to_str(value: u64) -> String {
        let s = format!("{:.8}", (value as f64) / MULTIPLIER);
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }

    #[allow(clippy::cast_possible_truncation)]
    #[allow(clippy::cast_sign_loss)]
    fn old_num_digits(value: u64) -> u32 {
        if value == 0 { 1 } else { (value as f64).log10().floor() as u32 + 1 }
    }

    // Prices and sizes below 4.5e7 whole units, i.e. everything HL lists.
    const EXACT_RANGE: u64 = 4_500_000_000_000_000;

    fn edge_values() -> Vec<u64> {
        let mut v = vec![0, 1, 9, 10, 11, 99_999_999, 100_000_000, 100_000_001, 123_456_789, 1_000_000_000];
        let mut p = 1u64;
        while p < EXACT_RANGE {
            v.extend([p - 1, p, p + 1, p * 5, p * 5 + 1]);
            p *= 10;
        }
        v
    }

    #[test]
    fn to_str_matches_float_formatting() {
        let mut rng = rand::rng();
        let mut values = edge_values();
        values.extend((0..2_000_000).map(|_| rng.random_range(0..EXACT_RANGE)));
        // realistic shapes: few significant digits at every magnitude
        values.extend((0..2_000_000).map(|_| {
            let sig = rng.random_range(1..100_000u64);
            sig * 10u64.pow(rng.random_range(0..11))
        }));
        for v in values.into_iter().filter(|&v| v < EXACT_RANGE) {
            assert_eq!(fixed_to_str(v), old_to_str(v), "value {v}");
            assert_eq!(Px::new(v).to_str(), Sz::new(v).to_str());
        }
    }

    #[test]
    fn to_str_is_exact_beyond_float_range() {
        assert_eq!(fixed_to_str(u64::MAX), "184467440737.09551615");
        assert_eq!(fixed_to_str(9_007_199_254_740_993), "90071992.54740993");
        assert_eq!(fixed_to_str(50), "0.0000005");
        assert_eq!(fixed_to_str(1_000_000_000_000), "10000");
    }

    // num_digits only buckets prices. The float version is already wrong at
    // 10^15 - 1 (log10 rounds up to 15.0), i.e. a ~$10M price, so compare
    // below 10^14 ($1M) and check exactness everywhere.
    #[test]
    fn num_digits_matches_float_log() {
        const PRICE_RANGE: u64 = 100_000_000_000_000;
        let mut rng = rand::rng();
        let mut values = edge_values();
        values.extend((0..2_000_000).map(|_| rng.random_range(0..PRICE_RANGE)));
        for &v in values.iter().filter(|&&v| v < PRICE_RANGE) {
            assert_eq!(Px::new(v).num_digits(), old_num_digits(v), "value {v}");
        }
        values.extend([999_999_999_999_999, u64::MAX]);
        for v in values {
            assert_eq!(Px::new(v).num_digits() as usize, v.to_string().len(), "value {v}");
        }
    }
}
