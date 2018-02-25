## @knitr metrics

his_loss <- new_metric("hisloss", "His loss function",
                        metric = function(model, out) {
                          return((model$mu - out$fit)^2)
})

her_loss <- new_metric("herloss", "Her loss function",
                        metric = function(model, out) {
                          return(abs(model$mu - out$fit))
                        })
