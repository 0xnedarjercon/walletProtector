import json
import os
from collections import defaultdict
import pickle
import copy
from solidityHelpers import LP, Gauge
from database import recordTable,  classifiedEvents, recordBalanceTable

hv = None
pd = None

# SELL = 0
# BUY = 1
# MINT = 2
# BURN = 3
# TRANSFER = 4
# APPROVAL = 5
# OTHER = 6
SELL = 'sell'
BUY = 'buy'
MINT = 'mint'
BURN = 'burn'
TRANSFER = 'tr'
GREWARDS = 'gr'
GDEPOSIT = 'gd'
GWITHDRAW = 'gw'
SEND = 'se'
RCV = 'rcv'
APPROVAL = 'ap'
OTHER = 'oth'
# classifyResults
BLOCKNUM = 0
TXHASH = 1
TYPE = 2
AMOUNT = 3
SENDERS = 4
RECEIVERS = 5
APPROVALS = 6

REWARD = 0
DEPOSIT = 1
WITHDRAW = 2
PRICE = 3
IN = 'in'
OUT = 'out'
ONWEEKINBLOCKS = 604800//2

processSettings = { 'rewardLP': {'0x7f670f78B17dEC44d5Ef68a48740b6f8849cc2e6':{'r0': 0, 'r1':0, 'isT1': True}}, 'token': '0xE642657E4F43e6DcF0bd73Ef24008394574Dee28', 'gauge': '0x8351616F224a035Aa5ee6b9f74A68659701af3e9', 'lps': {'0x0AA3E62f4d97C404012352E881a2D0f2712c24A2':{'r0':0, 'r1':0, 'isT1': True, 'fee': 0.003}, '0x445DeEbc5863a8Ae9e2Bdf7adceD6202509c5d5A':{'r0':0, 'r1':0, 'isT1': True}}, 'ignore':['0x8b2e016c878a223cba92576d081ea57df8bd4329', '0x0AA3E62f4d97C404012352E881a2D0f2712c24A2', '0x445DeEbc5863a8Ae9e2Bdf7adceD6202509c5d5A']}
29235509
32557446
def chunkRange(start, end, size):
    return zip(range(start, end, size), range(start + size, end + 1, size))

def lerp(x1, y1, x2, y2, x): 
  y = y1 + (x - x1) * (y2 - y1) / (x2 - x1)
  return y
# def lerp(a_val, a_idx, b_val, b_idx, x):
#     """Linear interpolation between two points (a_idx, a_val) and (b_idx, b_val) at x"""
#     return a_val + (b_val - a_val) * (x - a_idx) / (b_idx - a_idx)
def getAmountOut(amountIn, r0, r1, fee):
    amountIn -= amountIn*fee
    amountIn*r0/(r1+amountIn)

def getLiquidityOut(amountIn, r0, r1, fee, totalSupply):
    out = getAmountOut(amountIn*.49, r0, r1, fee)
    r0 += amountIn*.49
    r1 -= out
    return min((amountIn*.49 * totalSupply) / r0, (out * totalSupply) / r1)

sigDefault = {'p':0, 'n':0, 'avgt':0, 'avgsp':0, 't':[], 'sp':[]  }



class Risk:
    @classmethod
    def load(cls, filename):
        if not os.path.exists(filename):
            raise FileNotFoundError(f"File {filename} does not exist")
        try:
            with open(filename, 'rb') as f:
                obj = pickle.load(f)
            if not isinstance(obj, cls):
                raise TypeError(f"Loaded object is not an instance of {cls.__name__}")
            return obj
        except (pickle.UnpicklingError, EOFError) as e:
            raise pickle.UnpicklingError(f"Error loading state from {filename}: {e}")
        
    def __init__(self, dangerToken, gauge, rewardToken, safeToken, rewardLP, tradeLP, lps, ignore,  lookbackBlocks = 5000):
        self.dangerToken = dangerToken
        self.gauge = gauge
        self.rewardLP = rewardLP
        self.tradeLP = tradeLP
        self.rewardToken = rewardToken
        self.safeToken = safeToken
        self.lps = lps
        self.ignore = ignore
        self.usersEvents = {'all': {}}
        self.lastRisk = ''
        self.events = {}
        self.lookbackBlocks = lookbackBlocks        
        self.totalCounts = 0
        self.counts = {'all': {}}
        self.eventTimers = []
        self.totalStaked = 0
        self.totalRewardRate = 0
        self.incomePerLP = 0
        self.risks = []
        self.rewardPrice =0
        self.safeBalance = 0
        self.dangerbalance = 0
        self.lpBalance = 0
        self.state = 0
        self.currentRisks =  {}
        self.riskEvals = []
        self.prices = {}
        self.sells = {}
        self.userBalances = {}
        self.releventContracts =  [self.dangerToken, self.gauge.address, self.rewardLP.address, self.tradeLP.address, self.rewardToken]+list(self.lps.keys())
        self.riskCurves = {}
         

    def countEvents(self, startBlock, endBlock, classifiedTxDatabase, balanceDb):
        startChunk = startBlock
        while startChunk < endBlock:
            endChunk = min(startChunk+100000, endBlock)
            events = classifiedTxDatabase.fetch(f"""block_number between %s and %s ORDER BY block_number, tx_hash""", 
                                        [startChunk, endChunk])
            if len(events)>0:
                for event in events:
                    block, tx, typ, amount, senders, receivers, approvals = event
                    affectedUsers = []
                    self.events[block] = event
                    if len(self.eventTimers)>0:
                        self.checkExpiredTimers(block, self.lookbackBlocks)
                    for sender, amount in senders.items():
                        user = sender
                        if user not in self.lps:
                            affectedUsers.append(user)
                            if user not in self.usersEvents:
                                self.usersEvents[user] = []
                            if user not in self.counts:
                                self.counts[user] = {}
                            if user not in self.userBalances:
                                self.userBalances[user] = 0
                            if typ == SELL:
                                self.ProcessUserSell(user, block, event)
                            c =   [block, typ+'_snd', senders, approvals]              
                            self.usersEvents[user].append(c)
                            self.eventTimers.append([block, sender])     
                            self.userBalances[user]-= amount 
                            if self.userBalances[user]< 1000000000000000000:
                                self.usersEvents[user].append([block,'nb',[],[]])
                    for receiver, amount in receivers.items():
                        user = receiver
                        if user not in self.lps:
                            affectedUsers.append(user)
                            c =   [block, typ+'_rcv', receivers, approvals]
                            if user not in self.usersEvents:
                                self.usersEvents[user] = []
                            if user not in self.counts:
                                self.counts[user] = {}
                            if user not in self.userBalances:
                                self.userBalances[user] = 0
                            self.usersEvents[user].append(c)
                            self.eventTimers.append([block, receiver])
                            self.userBalances[user] += amount
                    if approvals and typ == APPROVAL:
                        user = approvals['owner']
                        affectedUsers.append(user)
                        if user not in self.usersEvents:
                            self.usersEvents[user] = []
                        if user not in self.counts:
                            self.counts[user] = {}
                        if user not in self.userBalances:
                                self.userBalances[user] = 0
                        self.eventTimers.append([block, user])
                        self.usersEvents[user].append([block, typ+approvals['spender'], [], approvals['spender']])
                        pass
            startChunk = endChunk+1
        for sig, data in self.counts['all'].items():
            if len(data['t']) >0:
                data['avgt'] = sum(data['t'])/len(data['t'])
                data['avgsp'] = sum(data['sp'])/len(data['sp'])
                data['tgt'] = data['avgsp']*data['p']/(data['p']+data['n'])
     
        for user in self.counts:
            self.counts[user] = dict(sorted(self.counts[user].items(), key=lambda x: x[1]['p'], reverse=True))
    def processCounts(self, events):
        for event in events:
            block, tx, typ, amount, senders, receivers, approvals = event
            senders = json.loads(senders)
            receivers = json.loads(receivers)
            approvals = json.loads(approvals)
            affectedUsers = []
            self.events[block] = event
            if len(self.eventTimers)>0:
                self.checkExpiredTimers(block, self.lookbackBlocks)
            for sender, amount in senders.items():
                user = sender
                if user not in self.lps:
                    affectedUsers.append(user)
                    if user not in self.usersEvents:
                        self.usersEvents[user] = []
                    if user not in self.counts:
                        self.counts[user] = {}
                    if user not in self.userBalances:
                        self.userBalances[user] = 0
                    if typ == SELL:
                        self.ProcessUserSell(user, block, senders)
                    c =   [block, typ+'_snd', senders, approvals]              
                    self.usersEvents[user].append(c)
                    self.eventTimers.append([block, sender])     
                    self.userBalances[user]-= amount 
                    if self.userBalances[user]< 1000000000000000000:
                        self.usersEvents[user].append([block,'nb',[],[]])
            for receiver, amount in receivers.items():
                user = receiver
                if user not in self.lps:
                    affectedUsers.append(user)
                    c =   [block, typ+'_rcv', receivers, approvals]
                    if user not in self.usersEvents:
                        self.usersEvents[user] = []
                    if user not in self.counts:
                        self.counts[user] = {}
                    if user not in self.userBalances:
                        self.userBalances[user] = 0
                    self.usersEvents[user].append(c)
                    self.eventTimers.append([block, receiver])
                    self.userBalances[user] += amount
            if approvals and typ == APPROVAL:
                user = approvals['owner']
                affectedUsers.append(user)
                if user not in self.usersEvents:
                    self.usersEvents[user] = []
                if user not in self.counts:
                    self.counts[user] = {}
                if user not in self.userBalances:
                        self.userBalances[user] = 0
                self.eventTimers.append([block, user])
                self.usersEvents[user].append([block, typ+approvals['spender'], [], approvals['spender']])
    
    
    def finaliseCounts(self):
        for sig, data in self.counts['all'].items():
            if len(data['t']) >0:
                data['avgt'] = sum(data['t'])/len(data['t'])
                data['avgsp'] = sum(data['sp'])/len(data['sp'])
                data['tgt'] = data['avgsp']*data['p']/(data['p']+data['n'])
        for user in self.counts:
            self.counts[user] = dict(sorted(self.counts[user].items(), key=lambda x: x[1]['p'], reverse=True))
                        



    def ensureUser(self, user):
        if user not in self.usersEvents:
            self.usersEvents[user] = []
        if user not in self.counts:
            self.counts[user] = {}
        if user not in self.userBalances:
            self.userBalances[user] = 0
  
  
    def analyseRisks(self, events):
        for event in events:
            block, tx, typ, amount, senders, receivers, approvals = event
            senders = json.loads(senders)
            receivers = json.loads(receivers)
            approvals = json.loads(approvals)
            amount = int(amount)
            if typ == BUY or typ == SELL:
                if typ == SELL:
                    self.sells[block] = -amount/self.tradeLP.r1
            affectedUsers = []
            self.events[block] = event
            for sender, amount in senders.items():
                user = sender
                if user not in self.lps:
                    affectedUsers.append(user)
                    self.ensureUser(user)
                    self.usersEvents[user].append([block, typ+'_snd', senders, approvals])    
                    self.userBalances[user]-= amount 
                    if self.userBalances[user]< 100000000000000000:
                        self.usersEvents[user].append([block,'nbbb',[],[]])
            for receiver, amount in receivers.items():
                user = receiver
                if user not in self.lps:
                    affectedUsers.append(user)
                    self.ensureUser(user)
                    self.usersEvents[user].append([block, typ+'_rcv', receivers, approvals])
                    self.userBalances[user] += amount
            if approvals and typ == APPROVAL:
                user = approvals['owner']
                affectedUsers.append(user)
                self.ensureUser(user)
                self.usersEvents[user].append([block, typ+approvals['spender'], [], approvals['spender']])
                pass
            if len(affectedUsers)>0:
                self.lastRisk = self.addRisks(affectedUsers, block)
            currentRisk = self.evaluateRisk(block)
            if len(self.riskEvals)>0 and self.riskEvals[-1][0] != block-1:
                self.riskEvals.append((block-1, self.evaluateRisk(block), ' '))
            if len(self.currentRisks)>0:
                self.riskEvals.append((block, currentRisk, f'{list(self.currentRisks.keys())[-1]}: {self.lastRisk}')) 
            for i in range(1,30):
                self.riskEvals.append((block+i, self.evaluateRisk(block), ' '))
        for sig, data in self.counts['all'].items():
            if len(data['t']) >0:
                data['avgt'] = sum(data['t'])/len(data['t'])
                data['avgsp'] = sum(data['sp'])/len(data['sp'])
                data['tgt'] = data['avgsp']*data['p']/(data['p']+data['n'])
     
        for user in self.counts:
            self.counts[user] = dict(sorted(self.counts[user].items(), key=lambda x: x[1]['p'], reverse=True))         
                

    def addRisks(self, users, block):
        lastRisk = ''
        for user in users:
            eventHistory, eventTimes, addressedEventHistory = self.getEventHistory(self.usersEvents[user], block - self.lookbackBlocks-1, block+1)
            eventHashes = self.getSigniatures(eventHistory, many = False)
            addressedEventHashes = self.getSigniatures(addressedEventHistory, many = False)
            risks = [0]*self.lookbackBlocks
            count = 0
            for eventHash in eventHashes:
                if eventHash in self.riskCurves:
                        risks =  [x+y for x,y in zip(risks,self.riskCurves[eventHash])] 
                        lastRisk = eventHash
                        count+= 1
            for eventHash in addressedEventHashes:
                if eventHash in self.riskCurves:
                        count+=1
                        risks =  [x+y for x,y in zip(risks,self.riskCurves[eventHash] )]
                        lastRisk = eventHash
            if risks[1]>0 and self.userBalances[user]>1000000000000000000:
                factor = self.userBalances[user]/count
                risks= [x*factor for x in risks]
            self.currentRisks[user] = (block, risks)
            return lastRisk

    def analyseEvent(self, user, block):
        eventHistory, eventTimes, addressedEventHistory = self.getEventHistory(self.usersEvents[user], block - self.lookbackBlocks-1, block+1)
        eventHashes = self.getSigniatures(eventHistory, many = False)
        addressedEventHashes = self.getSigniatures(addressedEventHistory, many = False)
        risks = []
        for eventHash in eventHashes:
            if eventHash in self.riskCurves:
                risks.append(self.riskCurves[eventHash])
        for eventHash in addressedEventHashes:
            if eventHash in self.riskCurves:
                risks.append(self.riskCurves[eventHash])
        b = self.dm.getBalanceAt(user, block)
        return eventHistory, eventHashes, addressedEventHashes, risks, b

    def evaluateRisk(self, block, remove = True):
        totalRisk = 0  
        toRemove = []
        for user, (riskStartBlock, finalRisk) in self.currentRisks.items(): 
            if len(finalRisk)>0: 
                if block-riskStartBlock > len(finalRisk)-1:
                    toRemove.append(user)
                else:
                    totalRisk += finalRisk[block-riskStartBlock]
        if remove:
            for user in toRemove:
                del self.currentRisks[user]
        return totalRisk


    def classifyEvents(self, data, updateRewards = False):
        results=[]
        sequence = {}
        block_number, tx, contract, eventType, event_index, to, fro, sender, eventData = data[0]
        currentTx = tx
        json.loads(eventData)
        res = [block_number, tx, None, 0, {}, {}, {}]
        for event in data:
            block_number, tx, contract, eventType, event_index, to, fro, sender, eventData = event
            if currentTx != tx:
                results.append(self.finaliseClassification(res, sequence))
                sequence = {}
                currentTx = tx
                res = [block_number, tx, None, 0, {}, {}, {}]
            if contract in self.lps:
                self.checkLPEvents(res, contract, eventType, eventData)
            elif contract == self.dangerToken:
                    self.checkTokenEvents(res, sequence,  eventType, to, fro, eventData)
        results.append(self.finaliseClassification(res, sequence))
        return results
                

    def finaliseClassification(self, res, sequence):
        for addr, val in sequence.items():
            if val >0:
                res[RECEIVERS][addr] = val
            elif val <0:
                res[SENDERS][addr] =- val

        if res[TYPE] is None: 
            if (len(res[SENDERS]) >0 or len(res[RECEIVERS]) >0):
                res[TYPE] = TRANSFER
            elif len(res[APPROVALS]) >0:
                res[TYPE] = APPROVAL
        res[APPROVALS] = json.dumps(res[APPROVALS])
        res[SENDERS] = json.dumps(res[SENDERS])
        res[RECEIVERS] = json.dumps(res[RECEIVERS])
        return res
    
    def checkLPEvents(self, res, contract, eventType, eventData):
            if eventType[:3] == "Swa":    
                if self.lps[contract].isT0(self.dangerToken):    
                    res[AMOUNT]+= eventData['amount0Out'] 
                    res[AMOUNT]-= eventData['amount0In']  
                else:
                    res[AMOUNT] += eventData['amount1Out'] 
                    res[AMOUNT] -= eventData['amount1In']                                  
            elif eventType[:3] == "Min":
                res[TYPE] = MINT
            elif eventType[:4] == "Burn":
                res[TYPE] = BURN
            if res[TYPE] not in [MINT, BURN]:
                if res[AMOUNT]< 0: 
                    res[TYPE] = SELL
                elif res[AMOUNT]>0:
                    res[TYPE] = BUY


    def checkTokenEvents(self, res, sequence, eventType, to, fro, eventData):
        
        if eventType[:4] == "Tran":
            if to not in self.ignore:
                if to not in sequence:
                    sequence[to] = 0
                sequence[to] += eventData['value']
            if fro not in self.ignore:    
                if fro not in sequence:
                    sequence[fro] = 0     
                sequence[fro] -= eventData['value']
        elif eventType[:3] == 'App':
            res[APPROVALS]=eventData
                  

    
    def checkRewardUpdates(self, txData):
        updateCalc = False
        for contractAddr, events in txData.items():
            if contractAddr == self.rewardLP.address:
                for eventName, eventData in events.items():
                    if eventName[:3] == 'Syn':
                        self.rewardPrice = self.rewardLP.getPrice(self.rewardToken)
                        updateCalc = True
            elif contractAddr ==self.gauge.address:
                for eventName, eventData in events.items():
                    if eventName[:3] == 'Dep':
                        self.gauge.totalStaked += eventData['amount']
                        updateCalc = True
                    elif eventName[:3] == 'Wit':
                        self.gauge.totalStaked -= eventData['amount']
                        updateCalc = True
                    elif eventName[:3] == 'Not':
                        self.gauge.totalRewardRate = eventData['amount']/ONWEEKINBLOCKS
                        updateCalc = True
            elif contractAddr == self.tradeLP.address:
                for eventName, eventData in events.items():
                    if eventName[:3] == 'Syn':                    
                        self.tradeLP.r0 = eventData['reserve0']
                        self.tradeLP.r1 = eventData['reserve1']
                        # self.AmountLiquidity = getLiquidityOut(self.amount, self.r0, self.r1, processSettings['lps'][contractAddr]['fee'], self.totalLp)
        if updateCalc:
            self.incomePerLP = self.gauge.rewardPerLP()*self.rewardLP.getPrice(self.rewardToken)
            

 

    def getSigniatures(self, events, many=True):
        if many:
            arr= [','.join(t for t in events[:len(events)-i]) for i in range(len(events))]         
        else:
            arr = [','.join(t for t in events)]
            if len(arr) == 1 and arr[0] == '':
                arr = []
        return arr
    



    # returns the events history and elapsed times from lastBlock for a user since the provided time frame, most recent first
    def getEventHistory(self, events, minBlock, maxBlock):
        history = []
        rcvHistory = []

        hasAddress = False
        elapsedTimes = []
        if len(events) == 0:
            return [], [], []
        i = 1
        currentBlock = events[-1][0]
        if len(events)>0:
            while currentBlock >= minBlock:
                if currentBlock <= maxBlock:
                    history.insert(0,events[-i][1])
                    if events[-i][1] == 'tr_rcv':
                        largestSender = max(events[-i][2], key=events[-i][2].get) 
                        rcvHistory.insert(0,events[-i][1]+largestSender)
                        hasAddress = True
                    else:
                        rcvHistory.insert(0,events[-i][1])

                    elapsedTimes.insert(0,maxBlock - events[-i][0])
                i+=1
                if i > len(events):
                    break
                currentBlock = (events[-i][0])
        if not hasAddress:
            rcvHistory = []

        
        return history, elapsedTimes, rcvHistory


    # checks for expired timers and increments no sell counters if no sells were detected from the user in the lookback window
    def checkExpiredTimers(self, currentBlock, lookback):
            expiryBlock = self.eventTimers[0][0] + lookback
            while expiryBlock < currentBlock:
                user = self.eventTimers[0][1]
                if user not in self.usersEvents:
                    self.usersEvents[user] = []
                eventHistory, eventTimes, addressedEventHistory = self.getEventHistory(self.usersEvents[user], expiryBlock - lookback, expiryBlock)
                # if no sells, incr negative counters
                if f'{SELL}_snd' not in eventHistory:       
                    eventHashes = self.getSigniatures(eventHistory, many = False)
                    addressedEventHashes = self.getSigniatures(addressedEventHistory, many = False)
                    if len(eventHashes) == 0:
                        eventHashes.append('')
                    self.incrementNegativeCounts(user, eventHashes)
                    self.incrementNegativeCounts(user, addressedEventHashes)
                self.eventTimers.pop(0) 
                if len(self.eventTimers)>0:
                    expiryBlock = self.eventTimers[0][0]+ lookback
                else:
                    return
            return
    def incrementNegativeCounts(self,user, eventHashes):
        for eventHash in eventHashes:
            if eventHash not in self.counts[user]:
                self.counts[user][eventHash] = copy.deepcopy(sigDefault)
            if eventHash not in self.counts['all']:
                self.counts['all'][eventHash] = copy.deepcopy(sigDefault)
            self.counts[user][eventHash]['n'] += 1
            self.counts['all'][eventHash]['n'] += 1 

    # gets signiature of users history and increments positive counters for them
    def ProcessUserSell(self, user, currentBlock, senders):
        eventHistory, elapsedTimes, addressedEventHistory = self.getEventHistory(self.usersEvents[user], currentBlock-self.lookbackBlocks-1, currentBlock)
        historySigniatures = self.getSigniatures(eventHistory, many = False)
        addressedHistorySigniatures = self.getSigniatures(addressedEventHistory, many = False)
        if len(elapsedTimes) == 0:
            elapsedTimes.append(self.lookbackBlocks)
        if len(historySigniatures) == 0:
            historySigniatures.append('')
        sells = (senders[user])
        self.incrementPositiveCounts(historySigniatures, user, sells, elapsedTimes)
        self.incrementPositiveCounts(addressedHistorySigniatures, user, sells, elapsedTimes)


    def incrementPositiveCounts(self, approveHistory, user, sells, elapsedTimes):
        if len(approveHistory)>0:
            if self.userBalances[user] != 0:
                for i in range(len(approveHistory)):
                    historySig = approveHistory[i]
                    if historySig not in self.counts[user]:
                            self.counts[user][historySig] = copy.deepcopy(sigDefault)
                    if historySig not in self.counts['all']:
                        self.counts['all'][historySig] = copy.deepcopy(sigDefault)
                    self.counts[user][historySig]['p'] += 1
                    self.counts['all'][historySig]['p'] += 1
                    self.counts[user][historySig]['t'].append(elapsedTimes[0])
                    sp = sells/self.userBalances[user]
                    if sp>1.001:
                        
                        sp = 1
                    self.counts[user][historySig]['sp'].append(sp)
                    self.counts['all'][historySig]['t'].append(elapsedTimes[0])
                    self.counts['all'][historySig]['sp'].append(sp)


    def saveUserData(self, dirPath):
        if not os.path.exists(dirPath):
            os.makedirs(dirPath)
        for user, data in self.usersEvents.items():
            pth = os.path.join(dirPath, f'{user[:5]}')
            if not os.path.exists(pth):
                os.makedirs(pth)
            with open(os.path.join(pth, f'{user}.json'), 'w') as f:
                json.dump(data, f, indent=4)
    
    def getReleventEvents(self, addr, blockNum):
        events = []
        if addr in self.usersEvents and len(self.usersEvents[addr]) > 0:
            i = -1
            currentBlock = self.usersEvents[addr][i][0]
            while (currentBlock) > ((blockNum)-self.lookbackBlocks):
                events.append(self.usersEvents[addr][i])
                i -= 1
                if abs(i) > len(self.usersEvents[addr]):
                    break
                currentBlock = self.usersEvents[addr][i][0]
        return events
    


    def savelearntData(self, filename):
        try:
            with open(filename, 'w') as f:
                json.dump(self.counts, f, indent=4)
        except IOError as e:
            raise IOError(f"Error saving learnt data to {filename}: {e}")
    
    def learn(self):
        print('learning...')
        for sig, data in self.counts['all'].items():
            if len(data['t']) >0:
                data['avgt'] = sum(data['t'])/len(data['t'])
                data['avgsp'] = sum(data['sp'])/len(data['sp'])
                data['tgt'] = data['avgsp']*data['p']/(data['p']+data['n'])
        self.counts['all'] = dict(sorted(self.counts['all'].items(), key=lambda x: x[1]['p']*x[1]['p']/(x[1]['n']+1), reverse=True))
        self.create_time_risk_profile(self.counts['all'])

    def create_time_risk_profile(self, indata, plot = False):
        for sig, data in indata.items():
            if data['p']+data['n']> 10 and len(data['t'])>0:
                targetMax = data['tgt']
                data['r'] = []
                combined = {}
                for t, sp in zip(data['t'], data['sp']):
                    combined[t] = combined.get(t, 0.0) + sp
                i = 1
                combined = dict(sorted(combined.items(), key=lambda x: x[0], reverse=True))
                summed = [0]*(self.lookbackBlocks+1)
                last = 0
                for time, val in combined.items():               
                    summed[time] = last+val
                    last = last+val
                j = 0
                orig = copy.copy(summed)
                summed.reverse()
                lastx = 0
                lasty = 0
                curx = 0
                cury = 0
                    
                while j < (len(summed)):
                    cury = summed[j]
                    if cury >0:
                        if lastx < j-1:
                            for k in range(lastx, j):
                                summed[k] = lerp(lastx, lasty, j, cury, k)
                        lastx = j
                        lasty = summed[j]
                    j+=1
                for k in range(lastx, j):
                    summed[k] = lasty
                i=0
                summed.reverse()
                normaliseValue = targetMax/summed[0]
                if normaliseValue > 0:
                    normalisedSummed = [x*normaliseValue for x in summed]
                risk_profile = {}
                for t in range(self.lookbackBlocks):
                    risk_profile[t] = {
                        "risk_score": normalisedSummed[t]
                        # Add proportion_users and avg_percentage_sold if needed
                    }
                times = list(risk_profile.keys())
                risk_scores = [info["risk_score"] for info in risk_profile.values()]
                self.riskCurves[sig] =  risk_scores

    

    def save(self, filename):
        try:
            with open(filename, 'wb') as f:
                pickle.dump(self, f)
        except IOError as e:
            raise IOError(f"Error saving state to {filename}: {e}")
    


def sort_dict_by_balance(data, output_file):
    
    # Sort the dictionary by balance in descending order
    sorted_data = dict(sorted(data.items(), key=lambda x: x[1]['balance'], reverse=True))
    
    # Save sorted data to a new JSON file
    with open(output_file, 'w') as f:
        json.dump(sorted_data, f, indent=4)
    
    return sorted_data



                # rewardData = checkRewards(txData)
                # r.checkRewardData(rewardData)

def checkRewards(txData):
    rewardData = [0,0,0,0]
    for contractAddr, events in txData.items():
        if contractAddr == processSettings['gauge']:
            for eventName, eventData in events.items():
                if eventName[:3] == 'Not':
                    rewardData[REWARD] = eventData['amount']
                elif eventName[:3] == 'Dep':
                    rewardData[DEPOSIT] = eventData['amount']
                elif eventName[:3] == 'Wit':
                    rewardData[DEPOSIT] = eventData['amount']
        elif contractAddr == processSettings['rewardLP']:
            for eventName, eventData in events.items():
                if eventName[:3] == 'Syn':
                    if processSettings['rewardLP']['isT0']:
                        rewardData[PRICE] = eventData['reserve0']/eventData['reserve1']
                    else:
                        rewardData[PRICE] = eventData['reserve1']/eventData['reserve0']
    return rewardData



RELEVENT_RISK_EVENTS = ['Transfer', 'Mint', 'Burn', 'Swap','Sync', 'Approval']
RELEVENT_R_EVENTS = ['Transfer', 'Mint', 'Burn', 'Swap','Sync', 'Approve', 'Deposit', 'Withdraw', 'NotifyReward']
processSettings = { 'rewardLP': {'0x7f670f78B17dEC44d5Ef68a48740b6f8849cc2e6':{'r0': 0, 'r1':0, 'isT1': True}},   'lps': {'0x0AA3E62f4d97C404012352E881a2D0f2712c24A2':{'r0':0, 'r1':0, 'isT1': True, 'fee': 0.003}, '0x445DeEbc5863a8Ae9e2Bdf7adceD6202509c5d5A':{'r0':0, 'r1':0, 'isT1': True}}, 'ignore':[]}



class DataManager:
    def __init__(self, name,r, startBlock, endBlock, rawDb, processedDb, balanceTable):
        self.s = []
        self.p = {}
        self.r = r
        self.startBlock = startBlock
        self.endBlock = endBlock
        self.rawDb = rawDb
        self.processedDb = processedDb
        self.balanceTable = balanceTable
        self.path = f'./dataProcessing/{name}/'
        self.r.dm = self
    
    def load(self,  include = ['e', 'r', 're', 'p', 's'], exclude = []):
        if 'p' in include and 'p' not in exclude:
            with open(self.path+'prices.pkl', 'rb') as f:
                self.p = pickle.load(f)
        if 's' in include and 's' not in exclude:
            with open(self.path+'sells.pkl', 'rb') as f:
                self.s = pickle.load(f)
        if 'e' in include  and 'e' not in exclude:
            with open(self.path+'counts.pkl', 'rb') as f:
                self.r.counts = pickle.load(f)
        if 'r' in include and 'r' not in exclude:
            with open(self.path+'riskCurves.pkl', 'rb') as f:
                self.r.riskCurves = pickle.load(f)
        if 're' in include and 're' not in exclude:
            with open(self.path+'riskEvals.pkl', 'rb') as f:
                self.r.riskEvals = pickle.load(f)
            with open(self.path+'usersEvents.pkl', 'rb') as f:
                self.r.usersEvents = pickle.load(f)

             
    def classifyRange(self):
        currentBlock = self.startBlock
        currentEnd = self.endBlock
        releventContracts =  [self.r.dangerToken, self.r.tradeLP.address]+list(self.r.lps.keys())
        while currentBlock < self.endBlock:
            currentEnd = min(currentBlock + 10000, self.endBlock)
            events = self.rawDb.fetch(f"""block_number between %s and %s and contract_address in ({', '.join(['%s'] * len(releventContracts))}) 
                                        and event_type in ({', '.join(['%s'] * len(RELEVENT_RISK_EVENTS))}) ORDER BY block_number, event_index""", 
                                        [currentBlock, currentEnd]+ releventContracts+ RELEVENT_RISK_EVENTS)
            if len(events)>0:
                results = self.r.classifyEvents(events)
                self.ProcessedDb.addTupleEvents(results)
            currentBlock = currentEnd

    def countEvents(self):
        if len(self.r.counts)>1:
            return
        else:
            startChunk = self.startBlock
            while startChunk < self.endBlock:
                endChunk = min(startChunk+100000, self.endBlock)
                events = self.processedDb.fetch(f"""block_number between %s and %s ORDER BY block_number, tx_hash""", 
                                            [startChunk, endChunk])
                if len(events)>0:
                    self.r.processCounts(events)
                startChunk = endChunk+1
            self.r.finaliseCounts()
            with open(self.path+'counts.pkl', 'wb') as f:
                pickle.dump(self.r.counts, f)

    def train(self):
        if self.r.riskEvals:
            return 
        elif not len(self.r.counts)>1:
            self.countEvents()
        self.r.create_time_risk_profile(self.r.counts['all'])
        with open(self.path+'riskCurves.pkl', 'wb') as f:
            pickle.dump(self.r.riskCurves, f)

    def getRiskEvals(self):
        if self.r.riskEvals:
            return self.r.riskEvals
        elif not self.r.riskCurves:
            self.train()
        print('getting risk evals')
        startChunk = self.startBlock
        while startChunk < self.endBlock:
            endChunk = min(startChunk+100000, self.endBlock)
            events = self.processedDb.fetch(f"""block_number between %s and %s ORDER BY block_number, tx_hash""", [startChunk, endChunk])
            if len(events)>0:
                self.r.analyseRisks(events)
            startChunk = endChunk+1
        with open(self.path+'riskEvals.pkl', 'wb') as f:
            pickle.dump(self.r.riskEvals, f)
        with open(self.path+'usersEvents.pkl', 'wb') as f:
            pickle.dump(self.r.usersEvents, f)
        return self.r.riskEvals

    def getPrices(self):
        if self.p:
            return self.p
        else:
            startChunk = self.startBlock
            while startChunk < self.endBlock:
                endChunk = min(startChunk+100000, self.endBlock)
                events = self.rawDb.fetch(f"""block_number between %s and %s and contract_address in ({', '.join(['%s'] * 1)}) 
                                            and event_type in ({', '.join(['%s'] * 1)}) ORDER BY block_number""", 
                                            [startChunk, endChunk, self.r.tradeLP.address, 'Sync'] , columns = 'block_number, event_data')   
                for event in events:
                    block, eventData = event
                    eventData = json.loads(eventData)
                    self.p[block] = eventData['reserve0']/eventData['reserve1']
                startChunk = endChunk
            with open(self.path+'prices.pkl', 'wb') as f:
                pickle.dump(self.p, f) 
        return self.p

    def getSells(self):
        if self.s:
            return self.s
        else:
            print('getting sells')
            startChunk = self.startBlock
            while startChunk < self.endBlock:
                endChunk = min(startChunk+100000, self.endBlock)
                events = self.processedDb.fetch(f"""block_number between %s and %s and "type" ='sell'
                                             ORDER BY block_number""", 
                                            [startChunk, endChunk])  
                for event in events:
                    block, tx, typ, amount, senders, recievers, approvals = event                        
                    if len(self.s)>0 and self.s[-1][0] < block-1:
                        self.s.append((block-1, 0, 'b'))
                    self.s.append((block, int(-amount), senders))
                    self.s.append((block+1, 0, 'a'))
                startChunk = endChunk
            with open(self.path+'sells.pkl', 'wb') as f:
                pickle.dump(self.s, f) 
        return self.s
    
    def generateDfs(self, save = True):
        
        global pd
        if not self.s:
            self.getSells()
        if not self.p:
            self.getPrices()
        if not self.r.riskEvals:
            self.getRiskEvals()
        if not pd:
            import pandas as pd
        print('generating Dataframes')
        df1 = (
        pd.DataFrame(self.r.riskEvals, columns=['block', 'risks', 'details'])
        .assign(
            block=lambda x: pd.to_numeric(x['block'], errors='coerce', downcast='integer'),
            r=lambda x: pd.to_numeric(x['risks'], errors='coerce')
        ).dropna(subset=['block', 'risks']).sort_values('block')
        .reset_index(drop=True))
        df1["block"] = df1["block"].astype(int)
        df1["risks"] = df1["risks"].astype(float)
        df1 = df1.sort_values("block").reset_index(drop=True)
        

        df2 = (
        pd.DataFrame(self.s, columns=['block', 'sells', 'seller']).assign(
            block=lambda x: pd.to_numeric(x['block'], errors='coerce', downcast='integer'),
            sells=lambda x: pd.to_numeric(x['sells'], errors='coerce') ).dropna(subset=['block', 'sells', 'seller']).sort_values('block')
        .reset_index(drop=True))
        df2["block"] = df2["block"].astype(int)
        df2 = df2.sort_values("block").reset_index(drop=True)
        

        df3 = pd.DataFrame(list(self.p.items()), columns=["block",'price'])
        df3 = df3.apply(pd.to_numeric, errors='coerce')
        df3["block"] = df3["block"].astype(int)
        df3 = df3.sort_values("block").reset_index(drop=True)
        if save:
            df3.to_parquet(self.path+'price.parquet')
            df2.to_parquet(self.path+'sells.parquet')
            df1.to_parquet(self.path+'risks.parquet')
        return df1, df2, df3
    
    def loadDfs(self):
        global pd
        if not pd:
            import pandas as pd
        if not pd:
            import pandas as pd
        df = pd.read_parquet(self.path+'risks.parquet')
        df2 = pd.read_parquet(self.path+'sells.parquet')
        df3 = pd.read_parquet(self.path+'prices.parquet') 
        return df, df2, df3
    
    def generatePlots(self, df1, df2, df3):
        width = 1200
        height = 700

        global hv
        if not hv:
            import holoviews as hv
            import hvplot.pandas
            from bokeh.models import Label
            from holoviews.streams import PointerXY
            hv.extension('bokeh')
            
        persistent_label = None
        colors = hv.Cycle('Category10').values[:4]
        plot1 = df1.hvplot.line(
        x="block", y='risks',
        label='risks',
        color=colors[0],
        width=width, height=height,
        title='risks',shared_axes =False
        )
        scatter_markers = df1.hvplot.scatter(x="block", y='risks', marker='d', size=12, color='purple', hover_cols = ['block', 'risks', 'details'] )

        plot2 = df2.hvplot.line(
            x="block", y='sells',
            label='sells',
            color=colors[1],
            width=width, height=height,
            title='sells',shared_axes =False, hover_cols = ['block', 'sells', 'seller'],
        )
        plot3 = df3.hvplot.step( x="block", y='price', label='price',color=colors[2], width=width, height=height, title='price',shared_axes =False, where = 'post')
            # scatter_markers = df.hvplot.scatter(x="block", y=name, marker='d', size=12, color='purple')

        finalPlot = plot1.opts(
                    yaxis='left',show_legend=True, multi_y=True) *scatter_markers* plot2.opts(yaxis='right',show_legend=True, multi_y=True)*plot3.opts(yaxis='right',show_legend=True, multi_y=True)
        final_plot = finalPlot.opts(
                logy=False,
                xlabel="Block Number",
                show_grid=True,
                legend_position='top_left', shared_axes =False, multi_y=True,
        )
        df1 = None
        df2 = None
        df3 = None
        hv.save(final_plot, self.path+ "block_value.html")

    def exportCsvs(self, start, end):
        import csv
        self.processedDb.exportCsv(f'{self.path}/proc.csv')
        self.rawDb.exportCsv(f'{self.path}/raw.csv')
        with open(f'{self.path}/risks.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(('block', 'risk', 'reason'))
            writer.writerows([x for x in self.r.riskEvals if x[0]> start and x[1]< end])

    def calculateBalances(self, startBlock, endBlock, token):
        currentBlock = startBlock
        currentEnd = endBlock
        userBalances = {}
        balancesData = []
        while currentBlock < endBlock:
            blockBalances = {}
            currentEnd = min(currentBlock + 1000000, endBlock)
            events = self.rawDb.fetch(f"""block_number between %s and %s and contract_address = %s and event_type = 'Transfer' ORDER BY block_number, event_index""", (currentBlock, currentEnd, token))
            b = []
            for event in events:
                updatedUsers = []
                block_number, tx, contract, eventType, event_index, to, fro, sender, eventData = event
                value = json.loads(eventData)['value']
                userBalances[fro] = userBalances.get(fro, 0) - value
                userBalances[to] = userBalances.get(to, 0) + value
                userBalances[to] += value
                b.append((block_number, to, str(userBalances[to])))
                b.append((block_number, fro, str(userBalances[fro])))        
            self.balanceTable.addTupleEvents(b)
            currentBlock = currentEnd

    def getBalanceAt(self, user, block):
        balance = self.balanceTable.fetch(f'''block_number <= %s and user_address = %s order by block_number DESC limit 1
                                ''', params=(block, user), columns = 'balance')
        return int(balance[0][0])
        
    def getDataAtBlock(self, block):
        res = {'p':[], 's':[], 'r':[]}
        if block in self.p:
            res['p'] = self.p[block]
        for s in self.s:
            if s[0] == block:
                res['s'].append(s)
            elif s[0]> block:
                break
        for r in self.r.riskEvals:
            if r[0] == block:
                res['r'].append(r)
            elif s[0]> block:
                break
        return res


if __name__ == "__main__":
    startBlock = 16088295
    endBlock = 37268683
    size = 100000
    WETH = '0x4200000000000000000000000000000000000006'
    safeToken = WETH
    dangerToken = '0xE642657E4F43e6DcF0bd73Ef24008394574Dee28'
    gauge = Gauge('0x8351616F224a035Aa5ee6b9f74A68659701af3e9')
    rewardToken = '0x940181a94A35A4569E4529A3CDfB74e38FD98631'
    lp2 =  LP('0x445DeEbc5863a8Ae9e2Bdf7adceD6202509c5d5A', safeToken, dangerToken, 30)
    tradeLP = LP('0x0AA3E62f4d97C404012352E881a2D0f2712c24A2', safeToken, dangerToken, 30)
    rewardLP =  LP('0x7f670f78B17dEC44d5Ef68a48740b6f8849cc2e6',WETH, rewardToken, 30)
    ignore = ['0x8b2e016c878a223cba92576d081ea57df8bd4329']
    lps = {'0x445DeEbc5863a8Ae9e2Bdf7adceD6202509c5d5A': lp2, '0x0AA3E62f4d97C404012352E881a2D0f2712c24A2': tradeLP, '0x3fF52Bf4F84Aa8dd19cCd4CA624dc46F3EFb62e6': None}
    dm = DataManager('record',Risk(dangerToken, gauge, rewardToken, safeToken, rewardLP, tradeLP, lps, ignore) ,startBlock, endBlock, recordTable, classifiedEvents, recordBalanceTable)
    # dm.calculateBalances(startBlock, endBlock, dangerToken)
    # dm.getBalanceAt('0x0000000000000000000000000000000000000000', 37268680)
    dm.load(exclude = ['re'])
    # dm.getSells()
    # # dm.exportCsvs(33512710, 37268683)
    dm.getRiskEvals()
    if False:
        d1, d2, d3 = dm.generateDfs()
    else:
        d1, d2, d3 = dm.loadDfs()
    final_plot = dm.generatePlots(d1, d2, d3)
    print('done')
    
    '{"0x011b0a055E02425461A1ae95B30F483c4fF05bE7": 3298606669615312401087908}'