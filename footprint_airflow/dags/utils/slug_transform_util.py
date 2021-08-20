

def transform_slug_to_out(slug):
    if slug == 'uniswap-v2':
        return 'uniswap'
    return slug


def tranform_slug_from_out(slug):
    if slug == 'uniswap':
        return 'uniswap-v2'
    return slug
