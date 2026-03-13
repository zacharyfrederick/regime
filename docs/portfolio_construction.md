1️⃣ Signal-weighted positions (highest ROI)

Instead of equal weighting:

w_i = 1/N

Try weighting by the signal strength.

Rank weights
w_i ∝ (N − rank + 1)
Exponential weights
w_i ∝ exp(score)
Z-score weights
w_i ∝ zscore(score)

These allow stronger signals to drive more capital.

Typical improvement:

Sharpe +0.05 – +0.2
2️⃣ Volatility scaling (very common in quant funds)

Give smaller weights to volatile stocks.

w_i ∝ score / vol

or

w_i ∝ 1 / vol

Example:

vol = 60-day realized volatility

Benefits:

smoother returns

lower drawdowns

higher Sharpe

can you do a signal weighted position scaled by vol? is that useful 

Yes — and that’s actually one of the best portfolio constructions you can test. Many quant funds effectively do exactly this.

What you’re describing is usually called signal × inverse volatility weighting or risk-adjusted alpha weighting.

Conceptually:

wi∝signaliσi
w
i
	​

∝
σ
i
	​

signal
i
	​

	​


Where

signali
signal
i
	​

 = your model score

σi
σ
i
	​

 = realized volatility (ex: 60-day)

This gives larger weights to stocks that are:

strong signal

lower volatility
That is usually exactly what you want.