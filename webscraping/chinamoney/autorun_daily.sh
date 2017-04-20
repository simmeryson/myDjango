#!/bin/bash

function init()
{
    source ~/.bash_profile
    pyenv activate django
    return 0
}

init
if [ $? -eq 0 ];then
python BillMarketDaily.py
python BondDebitAndCreditDaily.py
python BondsSaleDaily.py
python BuyoutRepoDaily.py
python CreditLendDaily.py
python InterestRateSwapDaily.py
python MoneyBenchmarkMarketDaily.py
python PledgeRepoDaily.py
python ../realestate/creprice/cities_price.py
python ../realestate/creprice/distrank_price.py
fi