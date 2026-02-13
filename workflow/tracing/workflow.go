package tracing

import (
	"context"
	"errors"
	"strings"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v3/util/logging"
	"github.com/argoproj/argo-workflows/v3/util/telemetry"
	"github.com/argoproj/argo-workflows/v3/util/wfcontext"
)

func workflowID(name, namespace string) string {
	return namespace + "/" + name
}

func debugTrace(ctx context.Context, traceType string, span *trace.Span) {
	logging.RequireLoggerFromContext(ctx).WithField("trace", (*span).SpanContext().SpanID()).Info(ctx, traceType)
}

func (trc *Tracing) createWorkflow(id string) (*workflowSpans, error) {
	trc.mutex.Lock()
	defer trc.mutex.Unlock()
	if _, ok := trc.workflows[id]; ok {
		return trc.workflows[id], errors.New("found an existing trace for a starting workflow")
	}
	trc.workflows[id] = &workflowSpans{
		nodes: make(map[string]nodeSpans),
	}
	return trc.workflows[id], nil
}

func (trc *Tracing) expectWorkflow(id string) (*workflowSpans, error) {
	trc.mutex.RLock()
	wf, ok := trc.workflows[id]
	trc.mutex.RUnlock()
	if !ok {
		wf, _ := trc.createWorkflow(id)
		return wf, errors.New("no existing trace for a running workflow")
	}
	return wf, nil
}

func (wfs *workflowSpans) createNode(id string) (nodeSpans, error) {
	wfs.mutex.Lock()
	defer wfs.mutex.Unlock()
	if _, ok := wfs.nodes[id]; ok {
		return wfs.nodes[id], errors.New("found an existing trace for a starting node")
	}
	wfs.nodes[id] = nodeSpans{}
	return wfs.nodes[id], nil
}

func (wfs *workflowSpans) expectNode(id string) (nodeSpans, error) {
	wfs.mutex.RLock()
	node, ok := wfs.nodes[id]
	wfs.mutex.RUnlock()
	if !ok {
		node, _ := wfs.createNode(id)
		return node, errors.New("no existing trace for a running node")
	}
	return node, nil
}

func (trc *Tracing) updateWorkflow(id string, spans *workflowSpans) {
	trc.mutex.Lock()
	defer trc.mutex.Unlock()
	trc.workflows[id] = spans
}

func (wfs *workflowSpans) updateNode(id string, spans nodeSpans) {
	wfs.mutex.Lock()
	defer wfs.mutex.Unlock()
	wfs.nodes[id] = spans
}

func (wfs *workflowSpans) deleteNode(id string) {
	wfs.mutex.Lock()
	defer wfs.mutex.Unlock()
	delete(wfs.nodes, id)
}

func (trc *Tracing) RecordStartWorkflow(ctx context.Context, name, namespace string) context.Context {
	logger := logging.RequireLoggerFromContext(ctx)
	logger.Info(ctx, "Trace StartWorkflow")
	id := workflowID(name, namespace)
	spans, err := trc.createWorkflow(id)
	if err != nil {
		logger.WithError(err).Error(ctx, "create workflow failed")
		return ctx
	}
	var ts trace.TraceState

	if ts, err = ts.Insert("workflow", id); err != nil {
		logger.Info(ctx, "Trace StartWorkflow failed")
		return ctx
	}
	traceID := telemetry.DeterministicTraceID(wfcontext.UIDList(ctx)...)
	spanID := telemetry.DeterministicSpanID(wfcontext.UIDList(ctx)...)
	ctx = trace.ContextWithRemoteSpanContext(ctx, trace.SpanContext{}.WithTraceState(ts))
	ctx, span := trc.StartWorkflow(ctx, traceID, spanID, name, namespace)

	debugTrace(ctx, "start", &span)
	spans.workflow = &span
	trc.updateWorkflow(id, spans)
	return ctx
}

func (trc *Tracing) ChangeWorkflowPhase(ctx context.Context, name, namespace string, phase wfv1.WorkflowPhase) {
	logger := logging.RequireLoggerFromContext(ctx)
	logger.Info(ctx, "Trace CWP")
	id := workflowID(name, namespace)
	wf, err := trc.expectWorkflow(id)
	if err != nil {
		logger.WithError(err).Error(ctx, "expect workflow failed")
		return
	}
	if wf.phase != nil {
		(*wf.phase).End()
		debugTrace(ctx, "end wf phase", wf.phase)
	}
	spanID := telemetry.DeterministicSpanID(name, namespace, string(phase))
	_, newSpan := trc.StartWorkflowPhase(ctx, spanID, string(phase))
	debugTrace(ctx, "start", &newSpan)
	wf.phase = &newSpan
	trc.updateWorkflow(id, wf)
}

func (wfs *workflowSpans) endNodes(ctx context.Context) {
	wfs.mutex.Lock()
	defer wfs.mutex.Unlock()
	for _, node := range wfs.nodes {
		node.endNode(ctx, wfv1.NodeSkipped)
	}
}

func (trc *Tracing) EndWorkflow(ctx context.Context, name, namespace string, phase wfv1.WorkflowPhase) {
	logger := logging.RequireLoggerFromContext(ctx)
	logger.Info(ctx, "Trace EndWorkflow")
	id := workflowID(name, namespace)
	wf, err := trc.expectWorkflow(id)
	if err != nil {
		logger.WithError(err).Error(ctx, "expect workflow failed")
		return
	}
	wf.endNodes(ctx)
	if wf.phase != nil {
		(*wf.phase).End()
		debugTrace(ctx, "end wf phase", wf.phase)
	} else {
		logger.Error(ctx, "Unexpectedly didn't find a phase span for ending workflow")
	}
	if wf.workflow != nil {
		switch phase {
		case wfv1.WorkflowPending, wfv1.WorkflowRunning, wfv1.WorkflowUnknown:
			// Unexpected end
			(*wf.workflow).SetStatus(codes.Error, `Unexpected phase`)
		case wfv1.WorkflowSucceeded:
			(*wf.workflow).SetStatus(codes.Ok, ``)
		case wfv1.WorkflowFailed:
			(*wf.workflow).SetStatus(codes.Error, `Failed`)
		case wfv1.WorkflowError:
			(*wf.workflow).SetStatus(codes.Error, `Error`)
		}
		(*wf.workflow).End()
		debugTrace(ctx, "end wf", wf.workflow)
		trc.mutex.Lock()
		defer trc.mutex.Unlock()
		delete(trc.workflows, id)
	} else {
		logger.Error(ctx, "Unexpectedly didn't find a workflow span for ending workflow")
	}
}

func (trc *Tracing) RecoverWorkflowContext(ctx context.Context, id string) context.Context {
	trc.mutex.RLock()
	defer trc.mutex.RUnlock()
	if span, ok := trc.workflows[id]; ok {
		if span.workflow != nil {
			return trace.ContextWithSpan(ctx, *span.workflow)
		}
	}
	return ctx
}

func (trc *Tracing) RecordStartNode(ctx context.Context, name, namespace string, nodeID string, nodeType string, phase wfv1.NodePhase, message string) context.Context {
	logger := logging.RequireLoggerFromContext(ctx)
	logger.Info(ctx, "Trace Start Node")
	wfID := namespace + "/" + name
	wf, err := trc.expectWorkflow(wfID)
	if err != nil {
		logger.WithError(err).Error(ctx, "expect workflow failed")
		return ctx
	}
	node, err := wf.createNode(nodeID)
	if err != nil {
		logger.WithError(err).Error(ctx, "create node failed")
		return ctx
	}
	spanID := telemetry.DeterministicSpanID(nodeID)
	nodeCtx, span := trc.StartNode(ctx, spanID, nodeID, name, namespace, nodeType)
	debugTrace(nodeCtx, "start", &span)
	node.node = &span
	wf.updateNode(nodeID, node)
	trc.ChangeNodePhase(nodeCtx, wfID, nodeID, phase, message)
	return nodeCtx
}

func phaseMessage(phase wfv1.NodePhase, message string) string {
	switch phase {
	case wfv1.NodePending:
		splitReason := strings.Split(message, `:`)
		if splitReason[0] == "PodInitializing" {
			return ""
		}
		return splitReason[0]
	default:
		return ""
	}
}

func (trc *Tracing) ChangeNodePhase(ctx context.Context, wfID string, nodeID string, phase wfv1.NodePhase, message string) {
	logger := logging.RequireLoggerFromContext(ctx)
	logger.Info(ctx, "Trace CNP")
	wf, err := trc.expectWorkflow(wfID)
	if err != nil {
		logger.WithError(err).Error(ctx, "expect workflow failed")
		return
	}
	node, err := wf.expectNode(nodeID)
	if err != nil {
		logger.WithError(err).Error(ctx, "expect node failed")
		return
	}
	shortMsg := phaseMessage(phase, message)
	spanOpts := []telemetry.NodePhaseSpanOption{}
	if shortMsg != "" {
		spanOpts = append(spanOpts, telemetry.WithMessage(shortMsg))
	}
	logger.WithFields(logging.Fields{"message": message, "shortMsg": shortMsg}).Info(ctx, "Trace CNP message")
	if node.phasePhase == phase && node.phaseMsg == shortMsg {
		logger.Info(ctx, "Trace CNP no change")
		return
	}
	node.endPhase(ctx)
	if node.node != nil {
		ctx = trace.ContextWithSpan(ctx, *node.node)
	}
	node.phasePhase = phase
	node.phaseMsg = shortMsg
	if phase.Fulfilled(nil) {
		trc.EndNode(ctx, wfID, nodeID, phase)
	} else {
		spanID := telemetry.DeterministicSpanID(nodeID, string(phase))
		_, span := trc.StartNodePhase(ctx, spanID, nodeID, string(phase), spanOpts...)
		debugTrace(ctx, "start", &span)
		node.phase = &span
	}
	wf.updateNode(nodeID, node)
}

func (node *nodeSpans) endPhase(ctx context.Context) {
	if node.phase != nil {
		(*node.phase).End()
		debugTrace(ctx, "end node phase", node.phase)
		node.phase = nil
	}
}

func (node *nodeSpans) endNode(ctx context.Context, phase wfv1.NodePhase) bool {
	node.endPhase(ctx)
	if node.node != nil {
		switch phase {
		case wfv1.NodePending, wfv1.NodeRunning, wfv1.NodeSkipped, wfv1.NodeOmitted:
			(*node.node).SetStatus(codes.Error, `Unexpected phase`)
		case wfv1.NodeSucceeded:
			(*node.node).SetStatus(codes.Ok, ``)
		case wfv1.NodeFailed:
			(*node.node).SetStatus(codes.Error, `Failed`)
		case wfv1.NodeError:
			(*node.node).SetStatus(codes.Error, `Error`)
		}
		(*node.node).End()
		debugTrace(ctx, "end node", node.node)
		node.node = nil
		return true
	}
	return false
}

func (trc *Tracing) EndNode(ctx context.Context, wfID string, nodeID string, phase wfv1.NodePhase) {
	logger := logging.RequireLoggerFromContext(ctx)
	logger.Info(ctx, "Trace End Node")
	wfs, err := trc.expectWorkflow(wfID)
	if err != nil {
		logger.WithError(err).Error(ctx, "expect workflow failed")
		return
	}
	node, err := wfs.expectNode(nodeID)
	if err != nil {
		logger.WithError(err).Error(ctx, "expect node failed")
		return
	}
	if node.endNode(ctx, phase) {
		wfs.updateNode(nodeID, node)
	}
	wfs.deleteNode(nodeID)
	// May happen on controller restart
	//	logging.RequireLoggerFromContext(ctx).WithFields(logging.RequireLoggerFromContext(ctx).Fields{"workflow": wfID, "node": nodeID}).Error("Unexpectedly couldn't find a trace for node which is ending")
}
