# Build filter expression
def build_filter_expr(data: dict) -> str | None:
    listing_type = data.get('listing_type') 
    location = data.get('location')
    min_price = data.get('min_price')
    max_price = data.get('max_price')
    min_bedrooms = data.get('min_bedrooms')

    filters = []

    if listing_type:
        filters.append(f'listing_type == "{listing_type}"')
    if min_price:
        filters.append(f'price_egp >= {min_price}')
    if max_price:
        filters.append(f'price_egp <= {max_price}')
    if location:
        location_map = {
            'alexandria': 'alexandria',
            'الإسكندرية': 'alexandria',
            'اسكندرية': 'alexandria',
            'الاسكندرية': 'alexandria',
            'cairo': 'cairo', 
            'القاهرة': 'cairo',
            'قاهرة': 'cairo'
        }
        normalized_location = location_map.get(location.lower(), location)
        filters.append(f'location == "{normalized_location}"')
    if min_bedrooms:
        filters.append(f'bedrooms >= {min_bedrooms}')

    return ' && '.join(filters) if filters else None
