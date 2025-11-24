

class LP:
    def __init__(self, address, t0, t1, fee):
        self.address = address
        self.t0 = t0
        self.t1 = t1
        self.r0 = 1
        self.r1 = 1
        self.fee = fee
        self.totalSupply = 10**3
    #TODO test delta behaviour
    def getAmountOut(self, tokenIn, amountIn, delta0 = 0, delta1 = 0):
        amountIn -= (amountIn*self.fee)//10000
        if tokenIn == self.t0:
            r0 = self.r0+delta0
            r1 = self.r1+delta1
        elif tokenIn == self.t1:
            r0 = self.r1+delta1
            r1 = self.r0+delta0
        else:
            raise ValueError('token not in this LP')
        return amountIn*r0//(r1+amountIn)
    
    
    def estimateLiquidity0(self, amount0):
        return amount0 *self.totalSupply/self.r0
    
    def getPrice(self, token):
        if token == self.t1:
            return self.r0/self.r1
        elif token == self.t0:
            return self.r1/self.r0
        else:
            raise ValueError('token not in this LP')
    def isT0(self, token):
        if token == self.t0:
            return True
        elif token == self.t1:
            return False
        else:
            raise ValueError('token not in this LP')
    #TODO test
    def estimatePriceChange(self, tokenIn, mySwapAmount, thierSwapAmount):
        originalOut = self.getAmountOut(tokenIn, mySwapAmount)
        thierOut = self.getAmountOut(tokenIn, thierSwapAmount)
        if tokenIn == self.t0:
            delta0 = thierSwapAmount
            delta1 = -thierOut
        elif tokenIn == self.t1:
            delta0 = -thierOut
            delta1 = thierSwapAmount
        else:
            raise ValueError('token not in this LP')
        newOut = self.getAmountOut(tokenIn, mySwapAmount, delta0, delta1)
        return newOut-originalOut

    def estimateTokensOut(self, liquidity, sellToken):
        amount0 = self.r0*liquidity/self.totalSupply
        amount1 = self.r1*liquidity/self.totalSupply
        if sellToken == self.t0:
            amountIn = amount0
            amountOut = amount1
        elif sellToken == self.t1:
            amountIn = amount1
            amountOut = amount1
        else:
            raise ValueError('token not in this LP')
        return amountOut + self.getAmountOut(sellToken, amountIn, -amount0, -amount1)

    def estimateLiquidityOut(self, tokenIn, amountIn):
        swapAmount = (amountIn*49)//100
        amountLeft = amountIn-swapAmount
        amountOut = self.getAmountOut(tokenIn, amountIn)
        if tokenIn == self.t0:
            reserveA = self.r0+swapAmount
            reserveB = self.r1-amountOut
            liqA = amountLeft
            liqB = amountOut
        elif tokenIn == self.t1:
            reserveA = self.r0-amountOut
            reserveB = self.r1+amountIn
            liqB = amountLeft
            liqA = amountOut
        amountBOptimal = self.quoteLiquidity(liqA, reserveA, reserveB);
        if amountBOptimal > liqB:
            amountAOptimal = self.quoteLiquidity(liqB, reserveB, reserveA)
            liqA = amountAOptimal
        else:
            liqB = amountBOptimal
        lp = self.estimateLiquidity0(liqA)
        return lp


    def quoteLiquidity(self, amountA, reserveB, reserveA):
        return (amountA * reserveB) / reserveA

class Gauge:
    def __init__(self, address):
        self.totalStaked = 1
        self.address = address
        self.totalRewardRate = 1

    def rewardRate(self, amountStaked):
        self.totalRewardRate*amountStaked//self.totalStaked

    def rewardPerLP(self):
        return self.totalRewardRate//self.totalStaked