import numpy as np
from scipy.stats import norm
import math


N_prime = norm.pdf
N = norm.cdf

def black_scholes_call(S, K, T, r, sigma):
    '''

    :param S: Asset price
    :param K: Strike price
    :param T: Time to maturity
    :param r: risk-free rate (treasury bills)
    :param sigma: volatility
    :return: call price
    '''

    ###standard black-scholes formula
    d1 = (np.log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    call = S * N(d1) -  N(d2)* K * np.exp(-r * T)
    return call

def vega(S, K, T, r, sigma):
    '''

    :param S: Asset price
    :param K: Strike price
    :param T: Time to Maturity
    :param r: risk-free rate (treasury bills)
    :param sigma: volatility
    :return: partial derivative w.r.t volatility
    '''

    ### calculating d1 from black scholes
    d1 = (np.log(S / K) + (r + sigma ** 2 / 2) * T) / (sigma * np.sqrt(T))

    
    vega = S  * np.sqrt(T) * N_prime(d1)
    return vega


def implied_volatility_call(C, S, K, T, r, tol=0.000000001,
                            max_iterations=100):
    '''

    :param C: Observed call price
    :param S: Asset price
    :param K: Strike Price
    :param T: Time to Maturity
    :param r: riskfree rate
    :param tol: error tolerance in result
    :param max_iterations: max iterations to update vol
    :return: implied volatility in percent
    '''
    
    # Use relative tolerance for better convergence with small prices
    # For OTM options with small C, absolute tolerance is too strict
    relative_tol = max(tol, C * 1e-6)  # At least 1e-6 relative to price

    ### assigning initial volatility estimate for input in Newton_rap procedure
    ### brenner formula for initial estimate of volatility
    sigma = math.sqrt(2 * math.pi / T) * (C / S)
    
    # For OTM options, improve initial estimate
    # If S < K (OTM), use a better approximation
    if S < K:
        # For OTM: use moneyness-adjusted estimate
        moneyness = S / K
        # Adjust initial guess: deeper OTM needs higher vol to justify price
        sigma = max(sigma, math.sqrt(-2 * math.log(moneyness) / T) * 0.5)
    
    # Ensure initial guess is reasonable
    sigma = max(sigma, 0.01)  # At least 1%
    sigma = min(sigma, 5.0)   # Cap at 500%

    for i in range(max_iterations):

        ### calculate difference between blackscholes price and market price with
        ### iteratively updated volality estimate
        diff = black_scholes_call(S, K, T, r, sigma) - C

        ###break if difference is less than specified tolerance level
        if abs(diff) < relative_tol:
            if __name__ == "__main__":
                print(f'found on {i}th iteration')
                print(f'difference is equal to {diff}')
            break

        ### use newton rapshon to update the estimate
        v = vega(S, K, T, r, sigma)
        
        # Handle zero or very small vega (common for deep OTM)
        if abs(v) < 1e-10:
            # Vega too small - use bisection-like step
            # Try a small fixed step in the right direction
            if diff > 0:
                sigma = sigma * 0.9  # Price too high, reduce vol
            else:
                sigma = sigma * 1.1  # Price too low, increase vol
            # Ensure bounds
            sigma = max(sigma, 0.001)
            sigma = min(sigma, 10.0)
        else:
            # Normal Newton-Raphson step
            sigma_new = sigma - diff / v
            
            # Bounds checking to prevent divergence
            if sigma_new <= 0:
                sigma = sigma * 0.5  # Halve if goes negative
            elif sigma_new > 10.0:
                sigma = min(sigma * 1.1, 10.0)  # Cap at 1000%
            else:
                sigma = sigma_new

    # Validate final result
    if sigma <= 0 or np.isnan(sigma) or np.isinf(sigma) or sigma > 10.0:
        return np.nan
    
    return sigma


# Test with JPM ticker: O:JPM190215C00085000
if __name__ == "__main__":
    # Inputs per quote (from the data):
    S = 102.5  # underlying_spot (in dollars)
    K = 85.0   # strike (in dollars) - ITM call
    T = 30.0 / 365.0  # days_to_expiry / 365
    r = 0.02   # risk-free rate (SOFR/Treasury - using 2% as reasonable for 2019)
    observed_price = 18.3  # premium_low (premium in dollars)
    
    print("=" * 80)
    print("IMPLIED VOLATILITY CALCULATION (NEWTON-RAPHSON)")
    print("=" * 80)
    print(f"Ticker: O:JPM190215C00085000")
    print(f"Date: 2019-01-16")
    print(f"Expiration: 2019-02-15")
    print(f"Option Type: ITM Call (Strike < Spot, ITM: YES)")
    print()
    print("Inputs:")
    print(f"  Observed Price (C, premium_low): {observed_price}")
    print(f"  Underlying Spot (S): {S}")
    print(f"  Strike (K): {K}")
    print(f"  Time to Expiry (T): {T:.6f} years ({30.0} days / 365)")
    print(f"  Risk-free Rate (r): {r} ({r*100}%)")
    print()
    print(f"  Intrinsic value: ${max(S - K, 0):.2f}")
    print(f"  Time value: ${observed_price - max(S - K, 0):.2f}")
    print()
    
    print("Calculating implied volatility using Newton-Raphson method...")
    print("Initial estimate: σ = √(2π/T) * (C/S)")
    print()
    
    try:
        iv = implied_volatility_call(observed_price, S, K, T, r)
        print()
        print(f"✅ Implied Volatility (σ_impl): {iv:.6f} ({iv*100:.2f}%)")
        print()
        
        # Verify by calculating Black-Scholes price with the implied volatility
        C_BS = black_scholes_call(S, K, T, r, iv)
        print(f"Verification:")
        print(f"  C_BS(S, K, T, r, σ_impl) = {C_BS:.6f}")
        print(f"  Observed Price (C) = {observed_price:.6f}")
        print(f"  Difference: {abs(C_BS - observed_price):.6f}")
        
    except Exception as e:
        print(f"❌ Error calculating implied volatility: {e}")
        import traceback
        traceback.print_exc()
    
    print("=" * 80)