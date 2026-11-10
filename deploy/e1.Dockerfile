# Local rehearsal: extends the existing OSI-only contextualizer image; no packages added.
FROM uns-auto-contextualizer:local
COPY libs /workspace/libs
COPY services/uns-auto-contextualizer/src /workspace/services/uns-auto-contextualizer/src
COPY services/actuation-event-validator/src /workspace/services/actuation-event-validator/src
COPY scripts/e1_processing.py /workspace/scripts/e1_processing.py
COPY config/eirvah-line-a.jsonld /etc/e1/ontology.jsonld
COPY deploy/k3s/base/actuation-event-validator/actuation-policy.yaml /etc/e1/policy.yaml
ENV PYTHONPATH="/workspace/services/actuation-event-validator/src:/workspace/scripts"
ENTRYPOINT ["/workspace/.venv/bin/python"]
