result <- dscrutils::dscquery('template_out', targets = c('simulate', 'analyze', 'score', 'score.error'))
print(aggregate(score.error ~ simulate + analyze + score, result, mean))
