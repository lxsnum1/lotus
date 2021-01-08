package storageadapter

import (
	"bytes"
	"errors"
	"math/rand"
	"sort"
	"testing"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/market"
	test "github.com/filecoin-project/lotus/chain/events/state/mock"
	"github.com/filecoin-project/lotus/chain/types"
	market2 "github.com/filecoin-project/specs-actors/v2/actors/builtin/market"
	tutils "github.com/filecoin-project/specs-actors/v2/support/testing"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

var errNotFound = errors.New("Could not find")

func TestGetCurrentDealInfo(t *testing.T) {
	ctx := context.Background()
	dummyCid, _ := cid.Parse("bafkqaaa")
	dummyCid2, _ := cid.Parse("bafkqaab")
	zeroDealID := abi.DealID(0)
	earlierDealID := abi.DealID(9)
	successDealID := abi.DealID(10)
	proposal := market.DealProposal{
		PieceCID:             dummyCid,
		PieceSize:            abi.PaddedPieceSize(100),
		Client:               tutils.NewActorAddr(t, "client"),
		Provider:             tutils.NewActorAddr(t, "provider"),
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   abi.NewTokenAmount(1),
		ClientCollateral:     abi.NewTokenAmount(1),
		Label:                "success",
	}
	otherProposal := market.DealProposal{
		PieceCID:             dummyCid2,
		PieceSize:            abi.PaddedPieceSize(100),
		Client:               tutils.NewActorAddr(t, "client"),
		Provider:             tutils.NewActorAddr(t, "provider"),
		StoragePricePerEpoch: abi.NewTokenAmount(1),
		ProviderCollateral:   abi.NewTokenAmount(1),
		ClientCollateral:     abi.NewTokenAmount(1),
		Label:                "other",
	}
	successDeal := &api.MarketDeal{
		Proposal: proposal,
		State: market.DealState{
			SectorStartEpoch: 1,
			LastUpdatedEpoch: 2,
		},
	}
	earlierDeal := &api.MarketDeal{
		Proposal: otherProposal,
		State: market.DealState{
			SectorStartEpoch: 1,
			LastUpdatedEpoch: 2,
		},
	}
	testCases := map[string]struct {
		searchMessageLookup *api.MsgLookup
		searchMessageErr    error
		marketDeals         map[abi.DealID]*api.MarketDeal
		publishCid          *cid.Cid
		expectedDealID      abi.DealID
		expectedMarketDeal  *api.MarketDeal
		expectedError       error
	}{
		"deal lookup succeeds": {
			publishCid: &dummyCid,
			searchMessageLookup: &api.MsgLookup{
				Receipt: types.MessageReceipt{
					ExitCode: exitcode.Ok,
					Return:   makePublishDealsReturnBytes(t, []abi.DealID{successDealID}),
				},
			},
			marketDeals: map[abi.DealID]*api.MarketDeal{
				successDealID: successDeal,
			},
			expectedDealID:     successDealID,
			expectedMarketDeal: successDeal,
		},
		"deal lookup succeeds two return values": {
			publishCid: &dummyCid,
			searchMessageLookup: &api.MsgLookup{
				Receipt: types.MessageReceipt{
					ExitCode: exitcode.Ok,
					Return:   makePublishDealsReturnBytes(t, []abi.DealID{earlierDealID, successDealID}),
				},
			},
			marketDeals: map[abi.DealID]*api.MarketDeal{
				earlierDealID: earlierDeal,
				successDealID: successDeal,
			},
			expectedDealID:     successDealID,
			expectedMarketDeal: successDeal,
		},
		"deal lookup fails missing proposal": {
			publishCid: &dummyCid,
			searchMessageLookup: &api.MsgLookup{
				Receipt: types.MessageReceipt{
					ExitCode: exitcode.Ok,
					Return:   makePublishDealsReturnBytes(t, []abi.DealID{earlierDealID}),
				},
			},
			marketDeals: map[abi.DealID]*api.MarketDeal{
				earlierDealID: earlierDeal,
			},
			expectedDealID: zeroDealID,
			expectedError:  xerrors.Errorf("could not find deal in publish deals message %s", dummyCid),
		},
		"deal lookup fails mismatch count of deals and return values": {
			publishCid: &dummyCid,
			searchMessageLookup: &api.MsgLookup{
				Receipt: types.MessageReceipt{
					ExitCode: exitcode.Ok,
					Return:   makePublishDealsReturnBytes(t, []abi.DealID{earlierDealID}),
				},
			},
			marketDeals: map[abi.DealID]*api.MarketDeal{
				earlierDealID: earlierDeal,
				successDealID: successDeal,
			},
			expectedDealID: zeroDealID,
			expectedError:  xerrors.Errorf("deal index 1 out of bounds of deals (len 1) in publish deals message %s", dummyCid),
		},
		"search message fails": {
			publishCid:       &dummyCid,
			searchMessageErr: errors.New("something went wrong"),
			expectedDealID:   zeroDealID,
			expectedError:    xerrors.Errorf("looking for publish deal message %s: search msg failed: something went wrong", dummyCid),
		},
		"return code not ok": {
			publishCid: &dummyCid,
			searchMessageLookup: &api.MsgLookup{
				Receipt: types.MessageReceipt{
					ExitCode: exitcode.ErrIllegalState,
				},
			},
			expectedDealID: zeroDealID,
			expectedError:  xerrors.Errorf("looking for publish deal message %s: non-ok exit code: %s", dummyCid, exitcode.ErrIllegalState),
		},
		"unable to unmarshal params": {
			publishCid: &dummyCid,
			searchMessageLookup: &api.MsgLookup{
				Receipt: types.MessageReceipt{
					ExitCode: exitcode.Ok,
					Return:   []byte("applesauce"),
				},
			},
			expectedDealID: zeroDealID,
			expectedError:  xerrors.Errorf("looking for publish deal message %s: unmarshalling message return: cbor input should be of type array", dummyCid),
		},
	}
	runTestCase := func(testCase string, data struct {
		searchMessageLookup *api.MsgLookup
		searchMessageErr    error
		marketDeals         map[abi.DealID]*api.MarketDeal
		publishCid          *cid.Cid
		expectedDealID      abi.DealID
		expectedMarketDeal  *api.MarketDeal
		expectedError       error
	}) {
		t.Run(testCase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			ts, err := test.MockTipset(address.TestAddress, rand.Uint64())
			require.NoError(t, err)
			marketDeals := make(map[marketDealKey]*api.MarketDeal)
			for dealID, deal := range data.marketDeals {
				marketDeals[marketDealKey{dealID, ts.Key()}] = deal
			}
			api := &mockGetCurrentDealInfoAPI{
				SearchMessageLookup: data.searchMessageLookup,
				SearchMessageErr:    data.searchMessageErr,
				MarketDeals:         marketDeals,
			}

			dealID, marketDeal, err := GetCurrentDealInfo(ctx, ts, api, proposal, data.publishCid)
			require.Equal(t, data.expectedDealID, dealID)
			require.Equal(t, data.expectedMarketDeal, marketDeal)
			if data.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, data.expectedError.Error())
			}
		})
	}
	for testCase, data := range testCases {
		runTestCase(testCase, data)
	}
}

type marketDealKey struct {
	abi.DealID
	types.TipSetKey
}

type mockGetCurrentDealInfoAPI struct {
	SearchMessageLookup *api.MsgLookup
	SearchMessageErr    error

	MarketDeals map[marketDealKey]*api.MarketDeal
}

func (mapi *mockGetCurrentDealInfoAPI) ChainGetMessage(ctx context.Context, c cid.Cid) (*types.Message, error) {
	var dealIDs []abi.DealID
	var deals []market2.ClientDealProposal
	for k, dl := range mapi.MarketDeals {
		dealIDs = append(dealIDs, k.DealID)
		deals = append(deals, market2.ClientDealProposal{
			Proposal: market2.DealProposal(dl.Proposal),
			ClientSignature: crypto.Signature{
				Data: []byte("foo bar cat dog"),
				Type: crypto.SigTypeBLS,
			},
		})
	}
	sort.SliceStable(deals, func(i, j int) bool {
		return dealIDs[i] < dealIDs[j]
	})
	buf := new(bytes.Buffer)
	params := market2.PublishStorageDealsParams{Deals: deals}
	err := params.MarshalCBOR(buf)
	if err != nil {
		panic(err)
	}
	return &types.Message{
		Params: buf.Bytes(),
	}, nil
}

func (mapi *mockGetCurrentDealInfoAPI) StateMarketStorageDeal(ctx context.Context, dealID abi.DealID, ts types.TipSetKey) (*api.MarketDeal, error) {
	deal, ok := mapi.MarketDeals[marketDealKey{dealID, ts}]
	if !ok {
		return nil, errNotFound
	}
	return deal, nil
}

func (mapi *mockGetCurrentDealInfoAPI) StateSearchMsg(ctx context.Context, c cid.Cid) (*api.MsgLookup, error) {
	if mapi.SearchMessageLookup == nil {
		return mapi.SearchMessageLookup, mapi.SearchMessageErr
	}

	lookup := *mapi.SearchMessageLookup
	lookup.Message = c
	return &lookup, mapi.SearchMessageErr
}

func (mapi *mockGetCurrentDealInfoAPI) StateLookupID(ctx context.Context, addr address.Address, ts types.TipSetKey) (address.Address, error) {
	return addr, nil
}

func makePublishDealsReturnBytes(t *testing.T, dealIDs []abi.DealID) []byte {
	buf := new(bytes.Buffer)
	dealsReturn := market.PublishStorageDealsReturn{
		IDs: dealIDs,
	}
	err := dealsReturn.MarshalCBOR(buf)
	require.NoError(t, err)
	return buf.Bytes()
}
