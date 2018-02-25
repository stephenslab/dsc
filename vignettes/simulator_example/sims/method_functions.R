## @knitr methods

my_method <- new_method("my-method", "My Method",
                        method = function(model, draw) {
                          list(fit = median(draw))
                        })

their_method <- new_method("their-method", "Their Method",
                           method = function(model, draw) {
                             list(fit = mean(draw))
                           })
