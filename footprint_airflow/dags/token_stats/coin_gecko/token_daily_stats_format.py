import pydash


def token_daily_stats_format(token_daily):
    symbol = pydash.get(token_daily, 'symbol')
    address = pydash.get(token_daily, 'address')
    coin_gecko_id = pydash.get(token_daily, 'coin_gecko_id')
    day = pydash.get(token_daily, 'day')
    trading_vol_24h = pydash.get(token_daily, 'trading_vol_24h')
    high_price_24h = pydash.get(token_daily, 'high_price_24h')
    low_price_24h = pydash.get(token_daily, 'low_price_24h')
    circulating_supply = pydash.get(token_daily, 'circulating_supply')
    facebook_likes = pydash.get(token_daily, 'facebook_likes')
    fdv_to_tvl_ratio = pydash.get(token_daily, 'fdv_to_tvl_ratio')
    fully_diluted_valuation = pydash.get(token_daily, 'fully_diluted_valuation')
    market_cap = pydash.get(token_daily, 'market_cap')
    market_cap_rank = pydash.get(token_daily, 'market_cap_rank')
    max_supply = pydash.get(token_daily, 'max_supply')
    mcap_to_tvl_ratio = pydash.get(token_daily, 'mcap_to_tvl_ratio')
    price = pydash.get(token_daily, 'price')
    reddit_accounts_active_48h = pydash.get(token_daily, 'reddit_accounts_active_48h')
    reddit_average_comments_48h = pydash.get(token_daily, 'reddit_average_comments_48h')
    reddit_average_posts_48h = pydash.get(token_daily, 'reddit_average_posts_48h')
    reddit_subscribers = pydash.get(token_daily, 'reddit_subscribers')
    telegram_channel_user_count = pydash.get(token_daily, 'telegram_channel_user_count')
    total_supply = pydash.get(token_daily, 'total_supply')
    total_value_locked = pydash.get(token_daily, 'total_value_locked')
    twitter_followers = pydash.get(token_daily, 'twitter_followers')
    created_at = pydash.get(token_daily, 'created_at')
    updated_at = pydash.get(token_daily, 'updated_at')
    return {
        'symbol': symbol,
        'address': address,
        'coin_gecko_id': coin_gecko_id,
        'day': day,
        'trading_vol_24h': trading_vol_24h,
        'high_price_24h': high_price_24h,
        'low_price_24h': low_price_24h,
        'circulating_supply': circulating_supply,
        'facebook_likes': facebook_likes,
        'fdv_to_tvl_ratio': fdv_to_tvl_ratio,
        'fully_diluted_valuation': fully_diluted_valuation,
        'market_cap': market_cap,
        'market_cap_rank': market_cap_rank,
        'max_supply': max_supply,
        'mcap_to_tvl_ratio': mcap_to_tvl_ratio,
        'price': price,
        'reddit_accounts_active_48h': reddit_accounts_active_48h,
        'reddit_average_comments_48h': reddit_average_comments_48h,
        'reddit_average_posts_48h': reddit_average_posts_48h,
        'reddit_subscribers': reddit_subscribers,
        'telegram_channel_user_count': telegram_channel_user_count,
        'total_supply': total_supply,
        'total_value_locked': total_value_locked,
        'twitter_followers': twitter_followers,
        'updated_at': updated_at,
        'created_at': created_at
    }
