package storageadapter

import (
	"bytes"
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	"github.com/filecoin-project/lotus/chain/types"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"
)

type getCurrentDealInfoAPI interface {
	ChainGetMessage(ctx context.Context, mc cid.Cid) (*types.Message, error)
	StateLookupID(context.Context, address.Address, types.TipSetKey) (address.Address, error)
	StateMarketStorageDeal(context.Context, abi.DealID, types.TipSetKey) (*api.MarketDeal, error)
	StateSearchMsg(context.Context, cid.Cid) (*api.MsgLookup, error)
}

// GetCurrentDealInfo gets the current deal state and deal ID.
// Note that the deal ID is assigned when the deal is published, so it may
// have changed if there was a reorg after the deal was published.
func GetCurrentDealInfo(ctx context.Context, ts *types.TipSet, api getCurrentDealInfoAPI, proposal market.DealProposal, publishCid *cid.Cid) (abi.DealID, *api.MarketDeal, error) {
	if publishCid == nil {
		return abi.DealID(0), nil, xerrors.Errorf("could not get deal info for nil publish deals CID")
	}

	// Lookup the deal ID by comparing the deal proposal to the proposals in
	// the publish deals message, and indexing into the message return value
	dealID, err := dealIDFromPublishDealsMsg(ctx, ts, api, proposal, *publishCid)
	if err != nil {
		return dealID, nil, err
	}

	// Lookup the deal state by deal ID
	marketDeal, err := api.StateMarketStorageDeal(ctx, dealID, ts.Key())
	if err == nil {
		// Make sure the retrieved deal proposal matches the target proposal
		equal, err := checkDealEquality(ctx, ts, api, proposal, marketDeal.Proposal)
		if err != nil {
			return dealID, nil, err
		}
		if !equal {
			return dealID, nil, xerrors.Errorf("Deal proposals did not match")
		}
	}
	return dealID, marketDeal, err
}

// findDealID looks up the publish deals message by cid, and finds the deal ID
// by looking at the message return value
func dealIDFromPublishDealsMsg(ctx context.Context, ts *types.TipSet, api getCurrentDealInfoAPI, proposal market.DealProposal, publishCid cid.Cid) (abi.DealID, error) {
	dealID := abi.DealID(0)

	// Get the return value of the publish deals message
	lookup, err := api.StateSearchMsg(ctx, publishCid)
	if err != nil {
		return dealID, xerrors.Errorf("looking for publish deal message %s: search msg failed: %w", publishCid, err)
	}

	if lookup.Receipt.ExitCode != exitcode.Ok {
		return dealID, xerrors.Errorf("looking for publish deal message %s: non-ok exit code: %s", publishCid, lookup.Receipt.ExitCode)
	}

	var retval market.PublishStorageDealsReturn
	if err := retval.UnmarshalCBOR(bytes.NewReader(lookup.Receipt.Return)); err != nil {
		return dealID, xerrors.Errorf("looking for publish deal message %s: unmarshalling message return: %w", publishCid, err)
	}

	// Get the parameters to the publish deals message
	publishCid = lookup.Message
	pubmsg, err := api.ChainGetMessage(ctx, publishCid)
	if err != nil {
		return dealID, xerrors.Errorf("getting publish deal message %s: %w", publishCid, err)
	}

	var pubDealsParams market2.PublishStorageDealsParams
	if err := pubDealsParams.UnmarshalCBOR(bytes.NewReader(pubmsg.Params)); err != nil {
		return dealID, xerrors.Errorf("unmarshalling publish deal message params for message %s: %w", publishCid, err)
	}

	// Scan through the deal proposals in the message parameters to find the
	// index of the target deal proposal
	dealIdx := -1
	for i, paramDeal := range pubDealsParams.Deals {
		eq, err := checkDealEquality(ctx, ts, api, proposal, market.DealProposal(paramDeal.Proposal))
		if err != nil {
			return dealID, xerrors.Errorf("comparing publish deal message %s proposal to deal proposal: %w", publishCid, err)
		}
		if eq {
			dealIdx = i
			break
		}
	}

	if dealIdx == -1 {
		return dealID, xerrors.Errorf("could not find deal in publish deals message %s", publishCid)
	}

	if dealIdx >= len(retval.IDs) {
		return dealID, xerrors.Errorf("deal index %d out of bounds of deals (len %d) in publish deals message %s",
			dealIdx, len(retval.IDs), publishCid)
	}

	return retval.IDs[dealIdx], nil
}

func checkDealEquality(ctx context.Context, ts *types.TipSet, api getCurrentDealInfoAPI, p1, p2 market.DealProposal) (bool, error) {
	p1ClientID, err := api.StateLookupID(ctx, p1.Client, ts.Key())
	if err != nil {
		return false, err
	}
	p2ClientID, err := api.StateLookupID(ctx, p2.Client, ts.Key())
	if err != nil {
		return false, err
	}
	return p1.PieceCID.Equals(p2.PieceCID) &&
		p1.PieceSize == p2.PieceSize &&
		p1.VerifiedDeal == p2.VerifiedDeal &&
		p1.Label == p2.Label &&
		p1.StartEpoch == p2.StartEpoch &&
		p1.EndEpoch == p2.EndEpoch &&
		p1.StoragePricePerEpoch.Equals(p2.StoragePricePerEpoch) &&
		p1.ProviderCollateral.Equals(p2.ProviderCollateral) &&
		p1.ClientCollateral.Equals(p2.ClientCollateral) &&
		p1.Provider == p2.Provider &&
		p1ClientID == p2ClientID, nil
}
