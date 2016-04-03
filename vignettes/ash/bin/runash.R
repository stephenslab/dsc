library(ashr)
ash.wrapper=function(input,args=NULL){
  if(is.null(args)){
    args=list(mixcompdist="halfuniform",method="fdr")
  }
  res = do.call(ash, args=c(list(betahat=input$betahat,sebetahat=input$sebetahat),args))
  return(res)
}
ash_data = ash.wrapper(input$input, list(mixcompdist = mixcompdist, optmethod = "mixEM"))
