pub fn fair_prob_up(
    current_price: f64,
    strike_price: f64,
    time_to_expiry_s: f64,
    sigma_annual: f64,
) -> Option<f64> {
    if !(current_price.is_finite() && current_price > 0.0) {
        return None;
    }
    if !(strike_price.is_finite() && strike_price > 0.0) {
        return None;
    }
    if !(time_to_expiry_s.is_finite() && time_to_expiry_s >= 0.0) {
        return None;
    }
    if !(sigma_annual.is_finite() && sigma_annual >= 0.0) {
        return None;
    }

    if time_to_expiry_s <= 0.0 || sigma_annual <= 0.0 {
        return Some(if current_price >= strike_price { 1.0 } else { 0.0 });
    }

    // Lognormal diffusion approximation with zero drift:
    // P(S_T >= K) = Phi( ln(S_now/K) / (sigma * sqrt(t)) )
    // Where sigma is per sqrt(second).
    let seconds_per_year: f64 = 365.25 * 24.0 * 3600.0;
    let sigma_sqrt_s = sigma_annual / seconds_per_year.sqrt();
    let denom = sigma_sqrt_s * time_to_expiry_s.sqrt();
    if !(denom.is_finite() && denom > 0.0) {
        return None;
    }

    let z = (current_price / strike_price).ln() / denom;
    Some(standard_normal_cdf(z))
}

fn standard_normal_cdf(z: f64) -> f64 {
    if !z.is_finite() {
        return if z.is_sign_positive() { 1.0 } else { 0.0 };
    }
    if z > 8.0 {
        return 1.0;
    }
    if z < -8.0 {
        return 0.0;
    }
    0.5 * (1.0 + erf(z / 2.0_f64.sqrt()))
}

// Abramowitz and Stegun 7.1.26 approximation.
fn erf(x: f64) -> f64 {
    if !x.is_finite() {
        return if x.is_sign_positive() { 1.0 } else { -1.0 };
    }

    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let x = x.abs();

    let a1 = 0.254_829_592;
    let a2 = -0.284_496_736;
    let a3 = 1.421_413_741;
    let a4 = -1.453_152_027;
    let a5 = 1.061_405_429;
    let p = 0.327_591_1;

    let t = 1.0 / (1.0 + p * x);
    let y = 1.0
        - (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t) * (-x * x).exp();
    sign * y
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() <= tol
    }

    #[test]
    fn fair_prob_is_half_at_the_money() {
        let p = fair_prob_up(100.0, 100.0, 60.0, 1.0).unwrap();
        assert!(approx(p, 0.5, 1e-3), "p={}", p);
    }

    #[test]
    fn fair_prob_increases_when_price_above_strike() {
        let p1 = fair_prob_up(100.0, 100.0, 60.0, 1.0).unwrap();
        let p2 = fair_prob_up(101.0, 100.0, 60.0, 1.0).unwrap();
        assert!(p2 > p1);
    }

    #[test]
    fn fair_prob_zero_vol_is_deterministic() {
        assert_eq!(fair_prob_up(101.0, 100.0, 60.0, 0.0).unwrap(), 1.0);
        assert_eq!(fair_prob_up(99.0, 100.0, 60.0, 0.0).unwrap(), 0.0);
    }
}
