import pandas as pd

LISTED_EVENT= 'listed'
UNLISTED_EVENT= 'unlisted'

def net_daily_propertyevents(prop_df: pd.DataFrame) -> tuple:
    
    to_listed= 0
    to_unlisted= 0

    events_kind = list(prop_df.kind)
    to_listed   += int(events_kind[0] == LISTED_EVENT)
    to_unlisted += int(events_kind[0] == UNLISTED_EVENT)

    for i, event_kind in enumerate(events_kind[:-1]):
        if event_kind != events_kind[i+1]:
            if  events_kind[i+1] == LISTED_EVENT:
                to_listed += 1
            elif events_kind[i+1] == UNLISTED_EVENT:
                to_unlisted += 1
        
    if to_listed != to_unlisted:
        return (to_listed > to_unlisted, to_unlisted > to_listed)
    else:
        return ()


if __name__ == "__main__":
    input_df = pd.DataFrame.from_dict({
        'prop_id': [1, 1, 1, 1  ],
        'kind': ['unlisted', 'listed', 'unlisted', 'unlisted']
    })

    print(net_daily_propertyevents(input_df) == ())

