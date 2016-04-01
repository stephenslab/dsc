##  replace the extreme values with limits
winsor1 <- function (x, fraction=.05)
{
   if(length(fraction) != 1 || fraction < 0 ||
         fraction > 0.5) {
      stop("bad value for 'fraction'")
   }
   lim <- quantile(x, probs=c(fraction, 1-fraction))
   x[ x < lim[1] ] <- lim[1]
   x[ x > lim[2] ] <- lim[2]
   return(x)
}
x = winsor1(x, fraction)
