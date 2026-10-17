use super::{Project, ProjectEvidence, project, project_evidence};

#[test]
fn own_cwd_states_preserve_reasons_and_longest_root() {
    let projects = vec![
        Project {
            name: "tide".into(),
            roots: vec!["/work/tide".into()],
        },
        Project {
            name: "nested".into(),
            roots: vec!["/work/tide/tools".into()],
        },
        Project {
            name: "ambiguous".into(),
            roots: vec!["/work/tide/tools".into()],
        },
    ];
    assert_eq!(
        project_evidence(Some("/work/tide/src"), &projects),
        ProjectEvidence::Known {
            project: "tide".into()
        }
    );
    assert_eq!(
        project_evidence(Some("/work/tide-neighbor"), &projects),
        ProjectEvidence::Unconfigured {
            reason: "no_configured_root".into()
        }
    );
    assert_eq!(
        project_evidence(Some("/work/tide/tools"), &projects),
        ProjectEvidence::Unknown {
            reason: "ambiguous_roots".into()
        }
    );
    assert_eq!(
        project_evidence(None, &projects),
        ProjectEvidence::Unknown {
            reason: "missing_cwd".into()
        }
    );
    for cwd in [
        "relative/tide",
        "/work/tide/../other",
        "/work/tide/./src",
        "/work/tide\0",
    ] {
        assert_eq!(
            project_evidence(Some(cwd), &projects),
            ProjectEvidence::Unknown {
                reason: "invalid_cwd".into()
            }
        );
        assert_eq!(project(cwd, &projects), None);
    }
}
