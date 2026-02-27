How could you do a semi supervised version. I can take fred labels for recessions or even just stock market down 10% as a feature on an autoamted basis. But that only works for some.
9:08 PM

This is exactly the right instinct and it's a more sophisticated approach than most regime work attempts.

What semi-supervised buys you

The pure unsupervised HMM finds statistically coherent clusters in your feature space but has no guarantee those clusters correspond to economically meaningful regimes. It might split what you'd call a single momentum regime into two states because early momentum and late momentum have slightly different vol signatures. Or it might merge what you'd think of as distinct accumulation and early momentum because the feature overlap is high. You end up post-hoc interpreting whatever it found and hoping it makes sense.

Semi-supervised learning anchors some of the states to known economic ground truth while letting the model discover the structure it finds naturally elsewhere. You're saying to the model — I know what these specific periods are, learn to recognize them, and use that knowledge to structure how you think about everything else.

The labeled signal sources you have

NBER recession dates are the gold standard macro label. They're lagged — NBER declares recessions months after they begin — but for training purposes that's fine because you're using them as historical labels not real-time signals. Every NBER recession period gets a stress or contraction label. Available directly from FRED as USREC, a binary series.

Market drawdown thresholds give you a price-based label that's available in real time without lag. SPY down more than 10% from its recent high is a correction. Down more than 20% is a bear market. These are crude but unambiguous. You compute them directly from your SEP data.

VIX regimes are another clean label. VIX above 30 is empirically associated with stress regimes across decades of market history. VIX below 15 is historically associated with complacent bull markets. These aren't perfect but they're directionally reliable and lag-free.

Yield curve inversion has a strong historical relationship with subsequent recession and market stress. 2-year above 10-year for a sustained period is a macro stress label with documented predictive validity.

Credit spread regimes from BAMLH0A0HYM2 — HY spreads above 600 basis points have historically coincided with systemic stress. Spreads below 350 with bull market regimes.

The semi-supervised architecture options

There are several ways to inject labels into the HMM framework and they have different tradeoffs.

The simplest approach is constrained initialization. Instead of initializing the HMM randomly you initialize the emission distributions using the statistics of your labeled periods. Compute the mean and variance of your feature vector during NBER recession periods — that becomes your initial stress state emission distribution. Compute feature statistics during strong bull markets — that becomes your momentum state. Let the EM algorithm refine from there. You're giving the model a warm start that's grounded in economic reality rather than a random one.
python

from hmmlearn import hmm
import numpy as np

# Compute empirical statistics for labeled periods
recession_mask = fred_recession_indicator == 1
bull_mask = (spy_drawdown < 0.05) & (spy_ret_12m > 0.15)

# Initialize means from labeled periods
stress_mean = features[recession_mask].mean(axis=0)
momentum_mean = features[bull_mask].mean(axis=0)

# Unknown regimes initialized from data clustering
from sklearn.cluster import KMeans
unlabeled_features = features[~recession_mask & ~bull_mask]
kmeans = KMeans(n_clusters=2).fit(unlabeled_features)
accum_mean = kmeans.cluster_centers_[0]
distrib_mean = kmeans.cluster_centers_[1]

# Set initial means
model = hmm.GaussianHMM(n_components=4, covariance_type='full')
model.means_init = np.array([
    accum_mean,
    momentum_mean, 
    distrib_mean,
    stress_mean
])

A more principled approach is the Input-Output HMM or the labeled HMM where you fix the state assignments for labeled observations during training and only estimate parameters from them rather than treating them as unknown. The EM algorithm uses your known labels to anchor the emission distributions while still learning transition probabilities from the full sequence.
python

# During E-step of EM, fix posterior for labeled observations
# Instead of soft assignments, labeled observations get hard assignments
def modified_e_step(model, features, labels):
    # Standard forward-backward for unlabeled
    posteriors = model.predict_proba(features)
    
    # Override with hard labels where known
    known_stress = labels == 'stress'
    known_momentum = labels == 'momentum'
    
    posteriors[known_stress] = [0, 0, 0, 1]    # force stress state
    posteriors[known_momentum] = [0, 1, 0, 0]  # force momentum state
    
    return posteriors

The most flexible approach is a hybrid generative-discriminative model. You train a discriminative classifier — gradient boosted or logistic regression — on your labeled periods to predict regime state from features. You then use those predictions as soft labels to initialize and constrain the HMM. The discriminative model handles the labeled portion well, the HMM handles the temporal structure and unlabeled portion.

The label propagation problem

Your labeled signals only cover specific regime types and specific time periods. NBER recessions label stress periods but only macro-driven stress, not company-specific stress. Market corrections label broad market regimes but don't distinguish between a healthy correction within a bull market and the beginning of a structural bear. You need a strategy for how labels propagate to individual stocks.

The cleanest approach is hierarchical label propagation. Your macro labels — NBER recession, SPY drawdown thresholds, VIX regime — constrain the macro regime model directly. Those macro regime labels then become features and soft constraints for the sector models. The sector regime labels become features and soft constraints for individual stock models.

A stock in a sector that is itself in a macro stress environment gets a prior toward stress that the individual stock model has to overcome with strong contrary evidence to assign a different label. This mirrors how regimes actually propagate in markets and makes the semi-supervised signal more nuanced than a binary hard constraint.

The temporal label problem

NBER recession labels are particularly tricky because they're declared with significant lag — the 2020 recession was declared months after it ended. For training purposes you use the NBER dates as ground truth for historical analysis. But the label assignment has a directionality issue — the recession started in February 2020 but NBER didn't declare it until June. Your model should have been detecting stress in February not June.

The solution is to use the NBER label as a validation anchor rather than a training label. You train the model, then check whether it identified the stress regime beginning around the actual economic onset rather than the declaration date. If it does, the semi-supervised initialization worked correctly. If it only finds stress after the NBER declaration your model is learning to recognize the declaration rather than the underlying economic conditions.

Labels for individual stock regimes

The macro labels don't cover everything you need. For individual stocks you want labels for company-specific regime events that have nothing to do with macro. Earnings-driven regime changes, competitive disruption, secular growth regime entries. These don't have clean external labels.

For these you can use a form of self-labeling. Run the pure unsupervised HMM first to get initial labels. For observations where the model is highly confident — posterior probability above 0.90 for a single state — treat those as pseudo-labels. Use those pseudo-labels plus your external macro labels as the semi-supervised training set for a refined model. Iterate two or three times. This is essentially expectation maximization with partial labels and it's well validated in the semi-supervised learning literature.

The Gaussian mixture model alternative

For the semi-supervised problem a Gaussian mixture model with partial labels is sometimes cleaner than an HMM because it doesn't require the temporal structure to be consistent with the labels. You model each regime as a Gaussian in feature space, anchor the stress and momentum Gaussians using your labeled periods, and let the EM algorithm find the remaining components.

The loss of temporal structure is a real cost — you're no longer modeling regime persistence directly. But you recover it by adding lagged regime probabilities as features, which the GMM then learns to use as autocorrelation signals. The result is nearly equivalent to the HMM with more flexibility in how you inject supervision.

What the labeled signals actually constrain

It's worth being precise about what your labels are telling the model versus what they're not.

NBER recession dates tell the model what feature combinations correspond to macro stress. They don't tell it what accumulation looks like. VIX above 30 tells it about stress volatility regimes. It doesn't distinguish accumulation from distribution. SPY drawdown tells it about bear market price action. It doesn't capture the early warning signature before the drawdown begins.

The semi-supervised model is therefore partially anchored and partially free. The stress state is well constrained by multiple label sources. The momentum state is moderately constrained by bull market labels. Accumulation and distribution are mostly learned unsupervised because you don't have clean external labels for them.

This is actually fine and reflects the underlying reality — stress and momentum are the regimes with the most external validation available. Accumulation and distribution are the subtler regime concepts that your feature engineering needs to find without labels. The semi-supervised approach anchors what can be anchored and lets the model discover what can't be labeled externally.

The validation implication

Semi-supervised models need additional validation specifically around the labeled anchors. You check not just that the model finds economically sensible regimes but that the labeled periods are correctly classified at high confidence. If your stress state isn't capturing NBER recessions with high posterior probability something is wrong with either your feature engineering or your initialization.

You also check that the model generalizes beyond the labeled periods correctly. The 2022 rate shock was not an NBER recession but should be detected as a stress or distribution regime by a well-trained model. If the model only finds stress during actual recessions and misses significant non-recessionary drawdowns your stress state is too narrowly defined around the training labels.

The product implication

The semi-supervised approach gives you something important for the product — explainability of the stress regime specifically. You can tell customers that the stress regime was validated against NBER recession dates and historical market corrections, which gives the classification scientific credibility. The other regime states are described as empirically derived from market behavior patterns. That combination of external validation for some states and data-driven discovery for others is a more honest and more compelling methodology description than either pure supervised or pure unsupervised alone.
