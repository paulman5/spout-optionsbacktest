import numpy as np
from scipy.stats import norm
import math


N_prime = norm.pdf
N = norm.cdf

def black_scholes_call(S, K, T, r, sigma):
    '''
    Black-Scholes formula for call option pricing.

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


def probability_itm(S, K, T, r, sigma):
    """
    Calculate the probability that a call option expires in the money.
    
    Formula: P(S_T > K) = N(d_2)
    Where: d_2 = (ln(S/K) + (r - ½σ²)T) / (σ√T)
    
    :param S: Spot price
    :param K: Strike price
    :param T: Time to expiry (years)
    :param r: Risk-free rate
    :param sigma: Implied volatility
    :return: Probability (0 to 1) that S_T > K
    """
    # Calculate d_2
    d2 = (np.log(S / K) + (r - 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    
    # Probability is N(d_2)
    prob_itm = N(d2)
    return prob_itm


def newton_step(f, x0):
    """
    Single step of Newton-Raphson method using numerical derivative.
    
    :param f: Function to find root of (f(x) = 0)
    :param x0: Current estimate
    :return: Next estimate
    """
    def df(x):
        """Numerical derivative approximation"""
        dx = max(0.00001, x0 * 0.001)  # Adaptive step size
        return (f(x + dx) - f(x)) / dx
    
    try:
        df_val = df(x0)
        # Handle zero or very small derivative (common for deep OTM)
        if abs(df_val) < 1e-10:
            # Use bisection-like step instead
            f_val = f(x0)
            if f_val > 0:
                return x0 * 0.9  # Price too high, reduce vol
            else:
                return x0 * 1.1  # Price too low, increase vol
        
        x_new = x0 - f(x0) / df_val
        
        # Bounds checking to prevent divergence
        if x_new <= 0:
            return x0 * 0.5  # Halve if goes negative
        if x_new > 10.0:
            return min(x0 * 1.1, 10.0)  # Cap at 1000%
        
        return x_new
    except (ZeroDivisionError, OverflowError, ValueError):
        # Fallback: small adjustment
        f_val = f(x0)
        if f_val > 0:
            return x0 * 0.9
        else:
            return x0 * 1.1


def newton(f, x0, tol=1e-8, max_iterations=100, price=None):
    """
    Newton-Raphson algorithm to find root of f(x) = 0.
    
    :param f: Function to find root of
    :param x0: Initial guess
    :param tol: Tolerance for convergence
    :param max_iterations: Maximum number of iterations
    :param price: Optional price for relative tolerance
    :return: Root of f(x) = 0
    """
    # Use relative tolerance for small prices (OTM options)
    if price is not None and price > 0:
        relative_tol = max(tol, price * 1e-6)
    else:
        relative_tol = tol
    
    for i in range(max_iterations):
        x_new = newton_step(f, x0)
        
        # Check for convergence (both absolute and relative)
        if abs(x_new - x0) < relative_tol:
            return x_new
        
        # Also check if function value is close to zero
        f_val = abs(f(x_new))
        if f_val < relative_tol:
            return x_new
        
        x0 = x_new
    
    return x0


def implied_volatility_call(C, S, K, T, r, tol=1e-8, max_iterations=100):
    '''
    Calculate implied volatility using Newton-Raphson with numerical derivative.
    Based on the French quant's implementation.

    :param C: Observed call price
    :param S: Asset price
    :param K: Strike Price
    :param T: Time to Maturity
    :param r: riskfree rate
    :param tol: error tolerance in result
    :param max_iterations: max iterations to update vol
    :return: implied volatility (decimal, e.g., 0.20 = 20%)
    '''

    # Define the function: CallPrice(vol) - TargetPrice = 0
    # We want to find vol such that BlackScholes(S, K, T, r, vol) = C
    CallPriceVol = lambda vol: black_scholes_call(S, K, T, r, vol) - C
    
    # Initial guess using Brenner formula
    init = math.sqrt(2 * math.pi / T) * (C / S)
    
    # For OTM options (S < K), improve initial estimate
    # Deep OTM options need higher vol to justify their price
    if S < K:
        moneyness = S / K
        # Adjust for moneyness: deeper OTM needs higher vol
        otm_adjustment = math.sqrt(-2 * math.log(moneyness) / T) * 0.3
        init = max(init, otm_adjustment)
    
    # Ensure initial guess is reasonable
    init = max(init, 0.01)  # At least 1%
    init = min(init, 5.0)   # Cap at 500%
    
    # Use Newton-Raphson to find root (pass price for relative tolerance)
    try:
        iv = newton(CallPriceVol, init, tol, max_iterations, price=C)
        
        # Validate result
        if iv <= 0 or np.isnan(iv) or np.isinf(iv):
            return np.nan
        
        if iv > 10.0:  # Cap at 1000%
            return np.nan
        
        # Final verification: check if the calculated price is close to observed
        C_calc = black_scholes_call(S, K, T, r, iv)
        if abs(C_calc - C) > max(C * 0.01, 0.01):  # 1% relative or $0.01 absolute
            return np.nan
        
        return iv
    except:
        return np.nan


# Test with example from the slide
if __name__ == "__main__":
    print("=" * 80)
    print("TESTING GREEKS2.PY - French Quant's Newton-Raphson Implementation")
    print("=" * 80)
    print()
    
    # Test 1: Example from slide
    print("Test 1: Example from slide")
    print("-" * 80)
    S = 100
    K = 105
    T = 0.5
    r = 0.02
    vol = 0.2
    
    # Calculate call price with known volatility
    C = black_scholes_call(S, K, T, r, vol)
    print(f"Parameters:")
    print(f"  S = {S}, K = {K}, T = {T}, r = {r}")
    print(f"  True volatility: {vol} ({vol*100}%)")
    print(f"  Call price (with vol={vol}): {C:.6f}")
    print()
    
    # Try to recover volatility
    init = 0.1
    print(f"Initial guess: {init}")
    print("Newton-Raphson iterations:")
    x0 = init
    CallPriceVol = lambda vol: black_scholes_call(S, K, T, r, vol) - C
    for i in range(4):
        print(f"  Iteration {i}: {x0:.14f}")
        x0 = newton_step(CallPriceVol, x0)
    
    print()
    iv = implied_volatility_call(C, S, K, T, r)
    print(f"Calculated IV: {iv:.14f} ({iv*100:.2f}%)")
    print(f"Difference from true vol: {abs(iv - vol):.10f}")
    print()
    
    # Test 2: User's test case
    print("Test 2: User's test case (C=18, S=100, K=115, T=1, r=0.05)")
    print("-" * 80)
    C = 18.0
    S = 100.0
    K = 115.0
    T = 1.0
    r = 0.05
    
    print(f"Parameters:")
    print(f"  C = {C}, S = {S}, K = {K}, T = {T}, r = {r}")
    print()
    
    iv = implied_volatility_call(C, S, K, T, r)
    print(f"Calculated IV: {iv:.6f} ({iv*100:.2f}%)")
    
    # Verify
    C_BS = black_scholes_call(S, K, T, r, iv)
    print(f"Verification: C_BS = {C_BS:.6f}, Observed = {C:.6f}, Diff = {abs(C_BS - C):.6f}")
    print()
    
    print("=" * 80)

