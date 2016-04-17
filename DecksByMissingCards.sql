select  ds.deckId,
        ds.deckIdentity,
        ds.side,
        ds.deckFaction,
        ds.deckName,
        ds.likes,
        ds.favourites,
        ds.comments,
        ds.weightedLikes,
        ds.weightedFavourites,
        ds.weightedComments,
        ds.dateCreated,
        sum(case when owned = 0 then dl.cardQty else 0 end) missingCards,
        count(distinct case when owned = 0 then cs.setId else null end) missingSets
from    deckSummary ds
            left outer join deckList dl on ds.deckId = dl.deckId
                left outer join card c on dl.cardId = c.cardId
                    left outer join cardSet cs on c.setId = cs.setId
group by ds.deckId,
        ds.deckIdentity,
        ds.side,
        ds.deckFaction,
        ds.deckName,
        ds.likes,
        ds.favourites,
        ds.comments,
        ds.weightedLikes,
        ds.weightedFavourites,
        ds.weightedComments,
        ds.dateCreated
having missingCards = 0;